package io.inbot.elasticsearch.bulkindexing;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.exceptions.JsonTypeMismatchException;
import io.inbot.datemath.DateMath;
import io.inbot.elasticsearch.client.EsAPIClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk index objects to elastic search. Be sure to close after use to flush any remaining objects (use try with
 * resources).
 */
public class BulkIndexer implements BulkIndexingOperations {

    private static final Logger LOG = LoggerFactory.getLogger(BulkIndexer.class);
    private static Pattern VERSION_CONFLICT_PATTERN = Pattern.compile("current \\[(\\d+)\\], provided \\[(\\d+)\\]");

    private final int batchSize;
    Lock lock = new ReentrantLock();
    private final String index;
    private final String type;
    private final Meter indexMeter;
    private final Meter updateMeter;
    private final Meter deleteMeter;
    private final Meter flushMeter;
    private final Meter errorMeter;

    ArrayList<EsBulkOperation> request = new ArrayList<EsBulkOperation>();
    AtomicLong count = new AtomicLong();
    AtomicLong totalErrors = new AtomicLong();
    AtomicLong indexed = new AtomicLong();

    private final ExecutorService executorService;

    private final EsAPIClient esAPIClient;

    private BulkIndexerStatusHandler statusHandler = new BulkIndexerStatusHandler() {
        @Override
        public void handleVersionConflict(String id, Function<JsonObject, JsonObject> updateFunction) {
            // you could handle the conflict by fetching the object, applying the update function and storing the result.
            LOG.warn("unhandled version conflict: " + id);
        }

        @Override
        public void fail(String reason) {
            LOG.warn("bulk index failure: "+ reason);
        };

        @Override
        public void error(String code, JsonObject details) {
            LOG.warn("bulk index failure: "+ code + ", " + details);
        };

        @Override
        public JsonObject status() {
            return object(field("status", "default handler"));
        }
    };

    private boolean refresh;

    /**
     * @param esAPIClient
     *            client
     * @param index
     *            index name
     * @param type
     *            type
     * @param batchSize
     *            amount of documents sent to es per request
     * @param threads
     *            if greater than 1, an executor is used to send requests concurrently to elasticsearch. This makes
     *            better use of
     *            elasticsearch ability to index concurrently on multiple shards and nodes.
     * @param refresh
     *            defaults to false; please don't set this to true in production code since this may cause frequent
     *            index refreshes
     */
    public BulkIndexer(EsAPIClient esAPIClient, String index, String type, int batchSize, int threads, boolean refresh) {
        this.esAPIClient = esAPIClient;
        this.index = index;
        this.type = type;
        this.refresh = refresh;
        Validate.isTrue(batchSize > 0, "batchSize must be greater than 0");
        this.batchSize = batchSize;
        indexMeter = new Meter();
        updateMeter = new Meter();
        deleteMeter = new Meter();
        flushMeter = new Meter();
        errorMeter = new Meter();

        // only create executor if there's more than one thread
        if(threads > 1) {
            AtomicLong threadCounter = new AtomicLong();
            executorService = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
            // use twice the capacity needed so there is enough tasks to keep the threads busy but block adding new
            // tasks otherwise
                    new LinkedBlockingQueue<Runnable>(threads * 2), r -> new Thread(r, "bulkindexer-" + threadCounter.incrementAndGet()),
                    // ensure we don't drop rejected tasks because the queue is full and instead use the current thread
                    // instead of
                    // letting it generate even more tasks.
                    new ThreadPoolExecutor.CallerRunsPolicy());

        } else {
            executorService = null;
        }
    }

    private String metricsPrefix() {
        return "bulkindex." + index + "." + type;
    }

    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        metrics.put(metricsPrefix() + ".indexrate", indexMeter);
        metrics.put(metricsPrefix() + ".updaterate", updateMeter);
        metrics.put(metricsPrefix() + ".errorrate", errorMeter);
        metrics.put(metricsPrefix() + ".flushrate", flushMeter);
        return metrics;
    }

    @Override
    public void setRefresh(boolean refresh) {
        this.refresh = refresh;
    }

    @Override
    public void setBulkIndexerStatusHandler(BulkIndexerStatusHandler statusHandler) {
        this.statusHandler = statusHandler;
        statusHandler.start();
    }

    /* (non-Javadoc)
     * @see io.inbot.es.client.BulkIndexingOperations#index(com.github.jsonj.JsonObject)
     */
    @Override
    public void index(JsonObject o) {
        index(o, null);
    }

    /* (non-Javadoc)
     * @see io.inbot.es.client.BulkIndexingOperations#index(com.github.jsonj.JsonObject, java.lang.String)
     */
    @Override
    public void index(JsonObject o, String parentId) {
        // make sure nobody else is modifying
        String id = o.getString("id");
        String t = type;
        String version = o.getString("_version");
        if(t == null) {
            t = o.getString("_type");
        }
        if(StringUtils.isBlank(t)) {
            throw new IllegalArgumentException("cannot determine type of object " + o.toString());
        }
        index(id, t, parentId, version, o);
    }

    /* (non-Javadoc)
     * @see io.inbot.es.client.BulkIndexingOperations#index(java.lang.String, java.lang.String, java.lang.String, java.lang.String, com.github.jsonj.JsonObject)
     */
    @Override
    @SuppressWarnings("unchecked")
    public void index(String id, String type, String parentId, String version, JsonObject object) {
        lock.lock();
        try {
            JsonObject attributes = object(field("_index", index), field("_type", type));
            if(StringUtils.isNotEmpty(id)) {
                attributes.put("_id", id);
            }
            if(StringUtils.isNotEmpty(parentId)) {
                attributes.put("parent", parentId);
            }
            if(refresh) {
                attributes.put("refresh", true);
            }
            JsonObject metadata = object(field("index", attributes));
            if(version != null) {
                metadata.getOrCreateObject("index").add(field("_version", version));
            }
            EsBulkOperation requestobject = new EsBulkOperation(metadata, object, null);

            request.add(requestobject);
            indexMeter.mark();
            count.incrementAndGet();
        } finally {
            lock.unlock();
        }
        flushIfNeeded();
    }

    @Override
    public void delete(String id) {
        delete(index,type,id);
    }

    @Override
    public void delete(String id, String parent) {
        delete(index,type,id);
    }

    public void delete(String index, String type, String id) {
        lock.lock();
        try {
            request.add(new EsBulkOperation(object(field("delete", object(field("_index",index),field("_type",type),field("_id",id)))), null, null));
            deleteMeter.mark();
            count.incrementAndGet();
        } finally {
            lock.unlock();
        }
        flushIfNeeded();
    }

    /* (non-Javadoc)
     * @see io.inbot.es.client.BulkIndexingOperations#update(java.lang.String, java.lang.String, java.lang.String, com.github.jsonj.JsonObject, java.util.function.Function)
     */
    @Override
    @SuppressWarnings("unchecked")
    public void update(String id, String version, String parentId, JsonObject oldObject, Function<JsonObject, JsonObject> transformFunction) {
        // if currentObject is out of date update will fail
        JsonObject changedObject = transformFunction.apply(oldObject.deepClone());
        if(!oldObject.equals(changedObject)) {
            lock.lock();
            try {
                oldObject.removeEmpty();
                changedObject.put("updated_at", DateMath.formatIsoDateNow());
                JsonObject attributes = object(field("_index", index), field("_type", type));
                if(StringUtils.isNotEmpty(id)) {
                    attributes.put("_id", id);
                }
                if(refresh) {
                    attributes.put("refresh", true);
                }
                if(StringUtils.isNotEmpty(parentId)) {
                    attributes.put("parent", parentId);
                }
                if(version != null) {
                    attributes.add(field("_version", version));
                }

                JsonObject meta = object(field("index", attributes));
                EsBulkOperation requestobject = new EsBulkOperation(meta,changedObject, transformFunction);

                request.add(requestobject);
                updateMeter.mark();
                count.incrementAndGet();
            } finally {
                lock.unlock();
            }
            flushIfNeeded();
        }
    }

    private void flushIfNeeded() {
        long currentCount = count.get();
        if(currentCount > 0 && currentCount % batchSize * 2 == 0) {
            flush();
        }
    }

    @Override
    public void flush() {
        // nobody should add to the list while we are flushing
        StringBuilder body = new StringBuilder();
        try {
            lock.lock();
            ArrayList<EsBulkOperation> currentRequestObjects = new ArrayList<EsBulkOperation>();
            if(request.size() > 0) {

                for(EsBulkOperation es : request) {
                    body.append(es.toString());
                    currentRequestObjects.add(es);
                }
                request.clear();
            }
            if(body.length() != 0) {
                final String finalBody = body.toString();
                Callable<Boolean> task = new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        long start = System.currentTimeMillis();
                        try {
                            long duration = System.currentTimeMillis() - start;
                            JsonObject responseBody = esAPIClient.bulkIndex(index, type, finalBody);
                            JsonArray items = responseBody.getArray("items");
                            int size = items.size();
                            int errors = 0;
                            if(items.size() > 0) {
                                for(JsonObject item : items.objects()) {
                                    try {
                                        JsonObject error = item.getObject("create", "error");
                                        if(error != null) {
                                            errorMeter.mark();
                                            errors++;
                                            LOG.warn(item.toString());
                                            totalErrors.incrementAndGet();
                                            statusHandler.error("create_problem", item);
                                        }
                                        error = item.getObject("index", "error");
                                        if(error != null) {
                                            errorMeter.mark();
                                            errors++;
                                            if("version_conflict_engine_exception".equals(error.getString("type"))) {
                                                String reason = error.getString("reason");
                                                Matcher matcher = VERSION_CONFLICT_PATTERN.matcher(reason);
                                                if(matcher.find()) {
                                                    String provided = matcher.group(2);
                                                    for(EsBulkOperation r: currentRequestObjects) {
                                                        String theId = item.getString("index","_id");
                                                        if(!r.isSameVersion(theId, provided)) {
                                                            statusHandler.handleVersionConflict(theId, r.transformFunction);
                                                        }
                                                    }
                                                }
                                            } else {
                                                LOG.warn(item.toString());
                                                totalErrors.incrementAndGet();
                                                statusHandler.error("index_problem", item);
                                            }
                                        }
                                        if(error == null) {
                                            flushMeter.mark();
                                            statusHandler.ok(item);
                                        }
                                    } catch (NullPointerException e) {
                                        errors++;
                                        errorMeter.mark();
                                        statusHandler.error("index_problem", item);
                                        LOG.error("item not OK wtf?!?! " + item, e);
                                    } catch(JsonTypeMismatchException e) {
                                        errors++;
                                        errorMeter.mark();
                                        statusHandler.error("index_problem", item);
                                        LOG.error("item not OK wtf?!?! " + item.prettyPrint(), e);
                                    }
                                }
                            }
                            LOG.debug("indexed " + index + '/' + type + ": " + (size - errors) + " failed " + errors + ", total: " + indexed.addAndGet(size)
                                    + " failed " + totalErrors + ", duration " + duration + "ms.");
                            return true;
                        } catch (Exception e) {
                            long duration = System.currentTimeMillis() - start;
                            LOG.error("flush error after " + duration + "ms." + e.getMessage(), e);
                            throw e;
                        }
                    }
                };
                if(executorService != null) {
                    executorService.submit(task);
                } else {
                    try {
                        task.call();
                    } catch (Exception e) {
                        statusHandler.fail("bulk index flush failed: " + e.getMessage());
                    }
                }
            }
        } finally {
            lock.unlock();
            statusHandler.flush();
        }

    }

    @Override
    public void close() throws IOException {
        flush();
        if(executorService != null) {
            executorService.shutdown();
            try {
                // give it one minute to finish indexing
                boolean ok = executorService.awaitTermination(20, TimeUnit.MINUTES);
                if(ok) {
                    LOG.debug(index + '/' + type + " indexed " + indexed.get() + " documents out of " + count.get() + " submitted");
                } else {
                    statusHandler.fail(index + '/' + type + " indexed " + indexed.get()
                            + " documents but executor timed out on termination! There are probably documents that were not indexed.");
                    LOG.error(index + '/' + type + " indexed " + indexed.get()
                            + " documents but executor timed out on termination! There are probably documents that were not indexed.");
                }
            } catch (InterruptedException e) {
                statusHandler.fail("executor shutdown was interrupted");
                LOG.error("executor shutdown was interrupted", e);
            }
        } else {
            LOG.debug(index + '/' + type + " indexed " + indexed.get() + " documents out of " + count.get() + " submitted");
        }
        statusHandler.done();
    }
}
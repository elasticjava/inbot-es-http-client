package io.inbot.elasticsearch.crud;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.JsonjCollectors;
import io.inbot.datemath.DateMath;
import io.inbot.elasticsearch.bulkindexing.BulkIndexer;
import io.inbot.elasticsearch.bulkindexing.BulkIndexerStatusHandler;
import io.inbot.elasticsearch.client.ElasticSearchIndex;
import io.inbot.elasticsearch.client.ElasticsearchType;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import io.inbot.elasticsearch.exceptions.EsNotFoundException;
import io.inbot.elasticsearch.exceptions.EsVersionConflictException;
import io.inbot.redis.RedisBackedCircularStack;
import io.inbot.utils.HashUtils;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic crud for any type of object. Simply specify the index and type. Does no object validation.
 */
public class EsCrudDao implements CrudOperations {
    private static final Logger LOG = LoggerFactory.getLogger(EsCrudDao.class);

    private final EsAPIClient client;

    public final ElasticSearchIndex index;
    public final String type;
    private final Timer createTimer;
    private final Timer updateTimer;
    private final Timer deleteTimer;
    private final Timer queryTimer;
    private final RedisBackedCircularStack circularStack;
    private final int updateRetries;

    private final Timer getTimer;

    public EsCrudDao(ElasticsearchType type, EsAPIClient client, RedisBackedCircularStack circularStack, int updateRetries) {
        this.circularStack = circularStack;
        this.updateRetries = updateRetries;
        this.index = type.index();
        this.type = type.type();
        this.client = client;

        getTimer = new Timer();
        createTimer = new Timer();
        updateTimer = new Timer();
        deleteTimer = new Timer();
        queryTimer = new Timer();
    }

    private String metricsPrefix() {
        return "dao."+index.aliasPrefix()+"."+type;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        metrics.put(metricsPrefix()+".create", createTimer);
        metrics.put(metricsPrefix()+".get", getTimer);
        metrics.put(metricsPrefix()+".update", updateTimer);
        metrics.put(metricsPrefix()+".delete", deleteTimer);
        metrics.put(metricsPrefix()+".query", queryTimer);
        return metrics;
    }

    @Override
    public JsonObject mapping() {
        return client.getMapping(index.readAlias(), type);
    }

    @Override
    public JsonObject create(JsonObject object, boolean replace) {
        try(Context time = createTimer.time()) {
            if (object.get("id") == null) {
                // if no id, we will generate one for you - full service included
                object.put("id", HashUtils.createId());
            }
            String now = DateMath.formatIsoDateNow();
            object.put("created_at",now);
            object.put("updated_at",now);
            object.remove("_version");
            object.removeEmpty();
            JsonObject esResponse = client.createObject(index.writeAlias(), type,object.getString("id"), null, object, replace);
            JsonObject response = object.deepClone();
            response.put("_version", esResponse.getString("_version"));
            response.put("id", esResponse.getString("_id"));
            return response;
        }
    }

    @Override
    public Set<String> recentlyModifiedIds() {
        List<String> list = circularStack.list(redisKey());
        Set<String> result = new LinkedHashSet<>(list);
        return result;
    }

    private String redisKey() {
        return "dao-"+index+"-"+type;
    }

    @Override
    public JsonObject update(String id, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f) {
        return update(id, f, 0, updateRetries, modifyUpdatedAt);
    }

    private JsonObject update(String id, Function<JsonObject, JsonObject> f, int tries, int maxRetries, boolean modifyUpdatedAt) {
        try(Context time = updateTimer.time()) {
            JsonObject object = get(id);
            if(object == null) {
                throw new IllegalStateException("object does not exist " + index.writeAlias()+'/'+type +"/"+id);
            }
            JsonObject changedObject = f.apply(object.deepClone());
            if(!object.equals(changedObject)) {
                changedObject.removeEmpty();
                try {
                    String objectId = changedObject.getString("id");
                    if(!id.equals(objectId)) {
                        throw new IllegalArgumentException("id_mismatch");
                    }

                    if(get(id) == null) {
                        throw new IllegalStateException(index.writeAlias()+'/'+type +"/"+id);
                    }

                    if(modifyUpdatedAt) {
                        changedObject.put("updated_at",DateMath.formatIsoDateNow());
                    }

                    JsonObject response = changedObject.deepClone();
                    JsonElement version = response.remove("_version");
                    if(version == null) {
                        throw new IllegalStateException("update object has no version");
                    }
                    String previousVersion = version.asString();
                    response.remove("_type");
                    JsonObject esResponse = client.updateObject(index.writeAlias(), type, objectId, previousVersion, response);
                    if(circularStack!=null) {
                        circularStack.add(redisKey(), id);
                    }
                    response.put("_version", esResponse.getString("_version"));
                    response.put("id", esResponse.getString("_id"));
                    JsonObject update = response;
                    return update;
                } catch (EsVersionConflictException e) {
                    if(tries< maxRetries) {
                        try {
                            // wait a bit to let the concurrent write op do its thing
                            Thread.sleep(tries*50 + RandomUtils.nextInt(0,50));
                        } catch (InterruptedException ex) {
                            throw new IllegalStateException(ex);
                        }

                        JsonObject updated = update(id,f,tries+1, maxRetries, modifyUpdatedAt);
                        return updated;
                    } else {
                        throw e;
                    }
                }
            } else {
                return object;
            }
        }
    }

    @Override
    public JsonObject get(boolean cached, String id) {
        return get(id);
    }

    @Override
    public JsonArray mget(boolean cached, String...ids) {
        JsonObject esResponse = client.getObjects(index.readAlias(), type, ids);
        return esResponse.getOrCreateArray("docs").mapObjects(doc -> {
            if(doc.get("found", false)) {
                JsonObject object = doc.getObject("_source");
                object.put("_version", doc.getString("_version"));
                object.put("id", doc.getString("_id"));
                return object;
            } else {
                return null;
            }
        }).filter(doc -> doc != null).collect(JsonjCollectors.array());
    }

    @Override
    public JsonObject get(String id) {
        try(Context time = getTimer.time()) {
            JsonObject esResponse = client.getObject(index.readAlias(), type, id);
            JsonObject object = esResponse.getObject("_source");
            object.put("_version", esResponse.getString("_version"));
            object.put("id", esResponse.getString("_id"));
            return object;
        } catch (EsNotFoundException e) {
            return null;
        }
    }

    @Override
    public void delete(String id) {
        try(Context time = deleteTimer.time()) {
            // FIXME add support for consistency check on version
            JsonObject object = get(id);
            if(object != null) {
                client.deleteObject(index.writeAlias(), type,object.getString("id"));
            } else {
                throw new EsNotFoundException(index.writeAlias()+'/'+type +"/"+id);
            }
        }

    }

    @Override
    public void deleteByQuery(JsonObject query) {
        client.deleteByQuery(index.writeAlias(), type, query);
    }

    @Override
    public PagedSearchResponse pagedSearch(JsonObject q, int size, int from, String...fields) {
        try(Context time = queryTimer.time()) {
            return client.pagedSearch(index.readAlias(), type, q, size, from, fields);
        }
    }

    @Override
    public PagedSearchResponse pagedSearch(JsonObject q, int size, int from) {
        try(Context time = queryTimer.time()) {
            return client.pagedSearch(index.readAlias(), type, q, size, from);
        }
    }

    @Override
    public IterableSearchResponse iterableSearch(JsonObject q, int pageSize, int ttlMinutes, boolean rawResults) {
        try(Context time = queryTimer.time()) {
            return client.iterableSearch(index.readAlias(), type, q, pageSize, ttlMinutes, rawResults);
        }
    }

    @Override
    public JsonObject searchUnique(JsonObject q) {
        try(Context time = queryTimer.time()) {
            return pagedSearch(q, 2, 0).getFirstResult();
        }
    }

    @Override
    public BulkIndexer bulkIndexer() {
        final EsCrudDao theDao=this;
        // use non threaded bulk indexer by default
        BulkIndexer bulkIndexer = client.bulkIndexer(index.writeAlias(), type, 100, 0);
        bulkIndexer.setBulkIndexerStatusHandler(new BulkIndexerStatusHandler() {
            @Override
            public void error(String code, JsonObject details) {
                LOG.warn("bulk index failure: " + details);
            }

            @Override
            public void fail(String reason) {
                LOG.warn("bulk index failure: " + reason) ;
            }

            @Override
            public void handleVersionConflict(String id, Function<JsonObject, JsonObject> f) {
                try {
                    theDao.update(id, true, f);
                } catch(RuntimeException e) {
                    LOG.warn("bulk update failed with a version conflict after " + updateRetries + " retries", e);
                }
            }

            @Override
            public JsonObject status() {
                return object(field("status","OK"));
            }

        });
        return bulkIndexer;
    }
}

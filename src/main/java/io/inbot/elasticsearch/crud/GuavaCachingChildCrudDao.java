package io.inbot.elasticsearch.crud;



import com.codahale.metrics.Metric;
import com.github.jsonj.JsonObject;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.inbot.elasticsearch.bulkindexing.BulkIndexerStatusHandler;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class GuavaCachingChildCrudDao implements ParentChildCrudOperations {

    private final ParentChildCrudOperations crudDao;
    private LoadingCache<ParentChild,Optional<JsonObject>> cache;

    private static class ParentChild {
        public final String parent;
        public final String child;

        public ParentChild(String parent, String child) {
            this.parent = parent;
            this.child = child;
        }

        public static ParentChild from(String parent, String child) {
            return new ParentChild(parent, child);
        }
    }

    public GuavaCachingChildCrudDao(ParentChildCrudOperations crudDao, int maxItems, int expireAfterWriteSeconds) {
        this.crudDao = crudDao;
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS)
                .maximumSize(maxItems)
                .softValues().build(new CacheLoader<ParentChild, Optional<JsonObject>>() {

                    @Override
                    public Optional<JsonObject> load(ParentChild key) throws Exception {
                        JsonObject object = crudDao.get(false, key.child, key.parent);
                        if(object == null) {
                            return Optional.empty();
                        } else {
                            return Optional.of(object);
                        }
                    }
                });
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        metrics.putAll(crudDao.getMetrics());
        return metrics;
    }

    @Override
    public JsonObject create(JsonObject object, String parentId, boolean replace) {
        JsonObject created = crudDao.create(object, parentId, false);
        cache.put(ParentChild.from(parentId, created.getString("id")), Optional.of(object));
        return created;
    }

    @Override
    public Set<String> recentlyModifiedIds(String parentId) {
        return crudDao.recentlyModifiedIds(parentId);
    }

    @Override
    public JsonObject update(String id, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f, String parentId) {
        JsonObject updated = crudDao.update(id, true, f, parentId);
        cache.put(ParentChild.from(parentId, id), Optional.of(updated));
        return updated;
    }

    @Override
    public JsonObject get(String id, String parentId) {
        try {
            Optional<JsonObject> maybeValue = cache.get(ParentChild.from(parentId, id));
            if(maybeValue.isPresent()) {
                return maybeValue.get().deepClone();
            } else {
                return null;
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException("unexpected error fetching value from cache", e);
        }
    }

    @Override
    public JsonObject get(boolean cached, String id, String parentId) {
        if(cached) {
            return get(id,parentId);
        } else {
            return crudDao.get(cached, id, parentId);
        }
    }

    @Override
    public void delete(String id, String parentId) {
        crudDao.delete(id, parentId);
        cache.invalidate(ParentChild.from(parentId, id));
    }

    @Override
    public PagedSearchResponse pagedSearch(JsonObject q, int size, int from) {
        PagedSearchResponse results = crudDao.pagedSearch(q, size, from);
        return results;
    }

    @Override
    public IterableSearchResponse iterableSearch(JsonObject q, int pageSize, int ttlMinutes, boolean rawResults) {
        IterableSearchResponse results = crudDao.iterableSearch(q, pageSize, ttlMinutes, rawResults);
        return results;
    }

    @Override
    public JsonObject searchUnique(JsonObject q) {
        JsonObject result = crudDao.searchUnique(q);
        return result;
    }

    @Override
    public BulkIndexingOperations bulkIndexer() {
        BulkIndexingOperations bulkIndexer = crudDao.bulkIndexer();

        BulkIndexingOperations cacheUpdatingBulkIndexer = new BulkIndexingOperations() {

            @Override
            public void close() throws IOException {
                bulkIndexer.close();
            }

            @Override
            public void update(String id, String version, String parentId, JsonObject object, Function<JsonObject, JsonObject> transformFunction) {
                bulkIndexer.update(id, version, parentId, object, transformFunction);
                cache.put(ParentChild.from(parentId, id), Optional.of(object));
            }

            @Override
            public void setBulkIndexerStatusHandler(BulkIndexerStatusHandler statusHandler) {
                bulkIndexer.setBulkIndexerStatusHandler(statusHandler);
            }

            @Override
            public void index(String id, String type, String parentId, String version, JsonObject object) {
                bulkIndexer.index(id, type, parentId, version, object);
                cache.put(ParentChild.from(parentId, id), Optional.of(object));
            }

            @Override
            public void index(JsonObject o, String parentId) {
                bulkIndexer.index(o, parentId);
                cache.put(ParentChild.from(parentId, o.getString("id")), Optional.of(o));
            }

            @Override
            public void index(JsonObject o) {
                throw new UnsupportedOperationException("parentId required");
            }

            @Override
            public void delete(String id) {
                throw new UnsupportedOperationException("parentId required");
            }

            @Override
            public void delete(String id, String parentId) {
                bulkIndexer.delete(id);
                cache.invalidate(ParentChild.from(parentId, id));
            }

            @Override
            public void flush() {
                bulkIndexer.flush();
            }

            @Override
            public void setRefresh(boolean b) {
                bulkIndexer.setRefresh(b);
            }
        };

        return cacheUpdatingBulkIndexer;
    }
}

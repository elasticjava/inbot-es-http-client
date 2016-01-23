package io.inbot.elasticsearch.crud;


import static com.github.jsonj.tools.JsonBuilder.array;

import com.codahale.metrics.Metric;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.inbot.elasticsearch.bulkindexing.BulkIndexerStatusHandler;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GuavaCachingCrudDao implements CrudOperations {

    private final CrudOperations crudDao;
    private LoadingCache<String,Optional<JsonObject>> cache;

    public GuavaCachingCrudDao(CrudOperations crudDao, int maxItems, int expireAfterWriteSeconds) {
        this.crudDao = crudDao;
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS)
                .maximumSize(maxItems)
                .softValues().build(new CacheLoader<String, Optional<JsonObject>>() {

                    @Override
                    public Optional<JsonObject> load(String key) throws Exception {
                        JsonObject o = crudDao.get(key);
                        if(o==null) {
                            return Optional.empty();
                        } else {
                            return Optional.of(o);
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
    public JsonObject create(JsonObject object, boolean replace) {
        JsonObject created = crudDao.create(object, replace);
        cache.put(object.getString("id"), Optional.of(created));
        return created;
    }

    @Override
    public Set<String> recentlyModifiedIds() {
        return crudDao.recentlyModifiedIds();
    }

    @Override
    public JsonObject update(String id, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f) {
        JsonObject updated = crudDao.update(id, true, f);
        cache.put(updated.getString("id"), Optional.of(updated));
        return updated;
    }

    @Override
    public JsonObject mapping() {
        return crudDao.mapping();
    }

    @Override
    public JsonObject get(String id) {
        try {
            Optional<JsonObject> maybeValue = cache.get(id);
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
    public JsonArray mget(boolean cached, String... ids) {
        if(cached) {
            JsonArray cachedObjects=array();
            for(String id:ids) {
                try {
                    Optional<JsonObject> maybeObject = cache.get(id);
                    if(maybeObject.isPresent()) {
                        cachedObjects.add(maybeObject.get());
                    }
                } catch (ExecutionException e) {
                    throw new IllegalStateException("unexpected error fetching value from cache", e);
                }
            }
            if(cachedObjects.size() == ids.length) {
                return cachedObjects;
            } else if(cachedObjects.size()==0) {
                JsonArray results = crudDao.mget(cached, ids);
                for(JsonObject result:results.objects()) {
                    cache.put(result.getString("id"), Optional.of(result));
                }
                return results;
            } else {
                // some objects were cached, fetch the rest and re-assemble the results
                Set<String> alreadyFetched = cachedObjects.streamObjects().map(o -> o.getString("id")).collect(Collectors.toSet());
                String[] filteredIds = Arrays.stream(ids).filter(id -> !alreadyFetched.contains(id)).toArray(size -> new String[size]);
                JsonArray nonCachedObjects = crudDao.mget(cached, filteredIds);
                JsonObject idMap=new JsonObject();
                for(JsonObject o: cachedObjects.objects()) {
                    idMap.put(o.getString("id"), o);
                }
                for(JsonObject o: nonCachedObjects.objects()) {
                    idMap.put(o.getString("id"), o);
                    cache.put(o.getString("id"), Optional.of(o));
                }
                JsonArray allObjects=array();
                for(String id:ids) {
                    JsonElement jsonElement = idMap.get(id);
                    if(jsonElement != null) {
                        allObjects.add(jsonElement);
                    }
                }
                return allObjects;
            }
        } else {
            return crudDao.mget(cached, ids);
        }    }

    @Override
    public JsonObject get(boolean cached, String id) {
        if(cached) {
            return get(id);
        } else {
            return crudDao.get(cached, id);
        }
    }

    @Override
    public void delete(String id) {
        crudDao.delete(id);
        cache.invalidate(id);
    }

    @Override
    public void deleteByQuery(JsonObject query) {
        crudDao.deleteByQuery(query);
    }

    @Override
    public PagedSearchResponse pagedSearch(JsonObject q, int size, int from) {
        PagedSearchResponse results = crudDao.pagedSearch(q, size, from);
        return results;
    }

    @Override
    public PagedSearchResponse pagedSearch(JsonObject q, int size, int from, String...fields) {
        PagedSearchResponse results = crudDao.pagedSearch(q, size, from, fields);
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
                cache.put(id, Optional.of(object));
            }

            @Override
            public void setBulkIndexerStatusHandler(BulkIndexerStatusHandler statusHandler) {
                bulkIndexer.setBulkIndexerStatusHandler(statusHandler);
            }

            @Override
            public void index(String id, String type, String parentId, String version, JsonObject object) {
                bulkIndexer.index(id, type, parentId, version, object);
                cache.put(id, Optional.of(object));
            }

            @Override
            public void index(JsonObject o, String parentId) {
                bulkIndexer.index(o, parentId);
                cache.put(o.getString("id"), Optional.of(o));
            }

            @Override
            public void index(JsonObject o) {
                bulkIndexer.index(o);
                cache.put(o.getString("id"), Optional.of(o));
            }

            @Override
            public void delete(String id) {
                bulkIndexer.delete(id);
                cache.invalidate(id);
            }

            @Override
            public void delete(String id, String parentId) {
                bulkIndexer.delete(id);
                cache.invalidate(id);
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

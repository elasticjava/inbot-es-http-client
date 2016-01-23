package io.inbot.elasticsearch.crud;

import static com.github.jsonj.tools.JsonBuilder.array;

import com.codahale.metrics.Metric;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import io.inbot.redis.RedisCache;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RedisCachingCrudDao implements CrudOperations {

    private final CrudOperations crudDao;
    private final RedisCache redisCache;

    public RedisCachingCrudDao(CrudOperations crudDao, RedisCache redisCache) {
        this.crudDao = crudDao;
        this.redisCache = redisCache;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        metrics.putAll(crudDao.getMetrics());
        metrics.putAll(redisCache.getMetrics());
        return metrics;
    }

    @Override
    public JsonObject create(JsonObject object, boolean replace) {
        JsonObject created = crudDao.create(object, replace);
        return created;
    }

    @Override
    public Set<String> recentlyModifiedIds() {
        return crudDao.recentlyModifiedIds();
    }

    @Override
    public JsonObject update(String id, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f) {
        JsonObject updated = crudDao.update(id, true, f);
        redisCache.delete(id);
        return updated;
    }

    @Override
    public JsonObject mapping() {
        return crudDao.mapping();
    }

    @Override
    public JsonObject get(boolean cached, String id) {
        if(cached) {
            return get(id);
        } else {
            return crudDao.get(cached, id);
        }
    }

    @Override
    public JsonObject get(String id) {
        Optional<JsonObject> optional = redisCache.get(id, k -> {
            return crudDao.get(k);
        });

        if (optional.isPresent()) {
            return optional.get();
        } else {
            return null;
        }
    }

    @Override
    public JsonArray mget(boolean cached, String... ids) {
        if(cached) {
            JsonArray cachedObjects = redisCache.mget(ids);
            if(cachedObjects.size() == ids.length) {
                return cachedObjects;
            } else if(cachedObjects.size()==0) {
                JsonArray results = crudDao.mget(cached, ids);
                for(JsonObject result:results.objects()) {
                    redisCache.put(result);
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
                    redisCache.put(o);
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
        }
    }

    @Override
    public void delete(String id) {
        crudDao.delete(id);
        redisCache.delete(id);
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
        return crudDao.bulkIndexer();
    }
}

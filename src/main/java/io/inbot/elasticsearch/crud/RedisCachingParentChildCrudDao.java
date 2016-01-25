package io.inbot.elasticsearch.crud;

import com.codahale.metrics.Metric;
import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import io.inbot.redis.RedisCache;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Simple redis cache that you can use to wrap an existing ParentChildCrudOperations instance. Handles cache invalidation for single updates/deletes but not bulk operations
 */
public class RedisCachingParentChildCrudDao implements ParentChildCrudOperations {

    private final ParentChildCrudOperations dao;
    private final RedisCache cache;

    public RedisCachingParentChildCrudDao(ParentChildCrudOperations dao, RedisCache cache) {
        this.dao = dao;
        this.cache = cache;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        metrics.putAll(dao.getMetrics());
        metrics.putAll(cache.getMetrics());
        return metrics;
    }

    @Override
    public JsonObject create(JsonObject object, String parentId, boolean replace) {
        JsonObject created = dao.create(object,parentId, false);
        return created;
    }

    @Override
    public JsonObject update(String objectId, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f, String parentId) {
        JsonObject updated = dao.update(objectId,true,f, parentId);
        cache.delete(objectId);
        return updated;
    }

    @Override
    public JsonObject get(boolean cached, String id, String parentId) {
        if(cached) {
            return get(id, parentId);
        } else {
            return dao.get(cached, id, parentId);
        }
    }

    @Override
    public JsonObject get(String id, String parentId) {
        Optional<JsonObject> optional = cache.get(id, key -> {
            return dao.get(key, parentId);
        });
        return optional.orElse(null);
    }

    @Override
    public void delete(String objectId, String parentId) {
        cache.delete(objectId);
        dao.get(objectId, parentId);
    }

    @Override
    public Set<String> recentlyModifiedIds(String parentId) {
        return dao.recentlyModifiedIds(parentId);
    }

    @Override
    public PagedSearchResponse pagedSearch(JsonObject q, int size, int from) {
        return dao.pagedSearch(q, size, from);
    }

    @Override
    public IterableSearchResponse iterableSearch(JsonObject q, int pageSize, int ttlMinutes, boolean rawResults) {
        return dao.iterableSearch(q, pageSize, ttlMinutes, rawResults);
    }

    @Override
    public JsonObject searchUnique(JsonObject q) {
        return dao.searchUnique(q);
    }

    @Override
    public BulkIndexingOperations bulkIndexer() {
        return dao.bulkIndexer();
    }
}

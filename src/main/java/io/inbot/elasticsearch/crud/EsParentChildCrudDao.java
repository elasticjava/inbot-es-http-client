package io.inbot.elasticsearch.crud;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.jsonj.JsonObject;
import io.inbot.datemath.DateMath;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.ElasticSearchIndex;
import io.inbot.elasticsearch.client.ElasticsearchType;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import io.inbot.elasticsearch.exceptions.EsNotFoundException;
import io.inbot.elasticsearch.exceptions.EsVersionConflictException;
import io.inbot.redis.RedisBackedCircularStack;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.RandomUtils;

/**
 * Generic crud for objects that have another object as the parent. Does no object validation.
 */
public final class EsParentChildCrudDao implements ParentChildCrudOperations {
    private final EsAPIClient client;

    public final ElasticSearchIndex index;
    public final String type;
    private final Timer createTimer;
    private final Timer updateTimer;
    private final Timer deleteTimer;
    private final Timer queryTimer;
    private final RedisBackedCircularStack recentlyModifiedIdsStack;
    private final int maxUpdateRetries;
    private final Timer getTimer;

    public EsParentChildCrudDao(ElasticsearchType type, EsAPIClient client, RedisBackedCircularStack recentlyModifiedIdsStack, int maxUpdateRetries) {
        this.recentlyModifiedIdsStack = recentlyModifiedIdsStack;
        this.maxUpdateRetries = maxUpdateRetries;
        this.index = type.index();
        this.type = type.type();
        this.client = client;

        createTimer = new Timer();
        getTimer = new Timer();
        updateTimer = new Timer();
        deleteTimer = new Timer();
        queryTimer = new Timer();
    }

    private String redisPrefix() {
        return "dao."+index.aliasPrefix()+"."+type;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        metrics.put(redisPrefix()+".create", createTimer);
        metrics.put(redisPrefix()+".get", queryTimer);
        metrics.put(redisPrefix()+".update", updateTimer);
        metrics.put(redisPrefix()+".delete", deleteTimer);
        metrics.put(redisPrefix()+".query", queryTimer);

        return metrics;
    }

    private void markModifiedInRedis(String objectId, String parentId) {
        recentlyModifiedIdsStack.add(redisKey(parentId), objectId);
    }

    private String redisKey(String parentId) {
        return "chdao-"+index.indexName()+"-"+type+"-"+parentId;
    }

    @Override
    public JsonObject create(JsonObject object, String parentId, boolean replace) {
        String objectId=object.getString("id");
        if (objectId == null) {
            throw new IllegalArgumentException("no_id");
        }
        String now = DateMath.formatIsoDateNow();
        object.put("created_at",now);
        object.put("updated_at",now);
        object.remove("_version");
        String id = object.getString("id");

        try(Context time = createTimer.time()) {
            JsonObject esResponse = client.createObject(index.writeAlias(), type,id, parentId, object, replace);
            JsonObject response = object.deepClone();
            response.put("_version", esResponse.getString("_version"));
            response.put("id", esResponse.getString("_id"));
            markModifiedInRedis(objectId, parentId);
            return response;
        }
    }

    @Override
    public JsonObject update(String objectId, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f, String parentId) {
        return update(objectId, f, parentId, 0, maxUpdateRetries, modifyUpdatedAt);
    }

    private JsonObject update(String objectId, Function<JsonObject, JsonObject> f, String parentId, int tries, int maxTries, boolean modifyUpdatedAt) {
        try(Context time = updateTimer.time()) {
            JsonObject object = get(false,objectId,parentId);
            if(object == null) {
                throw new IllegalStateException("object does not exist "+index.writeAlias()+'/'+type +"/"+objectId);
            }
            JsonObject changedObject = f.apply(object.deepClone());
            if(!object.equals(changedObject)) {
                // only update if something actually changed
                try {
                    String id = object.getString("id");
                    String objectId1 = changedObject.getString("id");
                    if(!id.equals(objectId1)) {
                        throw new IllegalArgumentException("id_mismatch");
                    }

                    if(get(id, parentId) == null) {
                        throw new IllegalStateException("object does not exist "+index.writeAlias()+'/'+type +"/"+objectId1);
                    }

                    changedObject.put("updated_at",DateMath.formatIsoDateNow());

                    JsonObject response = changedObject.deepClone();
                    String previousVersion = response.remove("_version").asString();
                    response.remove("_type");
                    JsonObject esResponse = client.updateObject(index.writeAlias(), type, objectId1, parentId, previousVersion, response);
                    markModifiedInRedis(objectId1, parentId);
                    response.put("_version", esResponse.getString("_version"));
                    response.put("id", esResponse.getString("_id"));
                    JsonObject update = response;
                    return update;
                } catch (EsVersionConflictException e) {
                    if(tries<maxTries) {
                        try {
                            // wait a bit to let the concurrent write op do its thing
                            Thread.sleep(tries*50 + RandomUtils.nextInt(0,50));
                        } catch (InterruptedException ex) {
                            throw new IllegalStateException(ex);
                        }
                        return update(objectId, f, parentId, tries+1, maxTries, modifyUpdatedAt);

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
    public JsonObject get(boolean cached, String id, String parentId) {
        return get(id, parentId);
    }

    @Override
    public JsonObject get(String id, String parentId) {
        try(Context time = getTimer.time()) {
            JsonObject esResponse = client.getObject(index.readAlias(), type, id, parentId);
            JsonObject object = esResponse.getObject("_source");
            object.put("_version", esResponse.getString("_version"));
            object.put("id", esResponse.getString("_id"));
            return object;
        } catch (EsNotFoundException e) {
            return null;
        }
    }

    @Override
    public void delete(String objectId, String parentId) {
        try(Context time = deleteTimer.time()) {
            JsonObject object = get(objectId, parentId);
            if(object != null) {
                client.deleteObject(index.writeAlias(), type,object.getString("id"), null, parentId);
                markModifiedInRedis(objectId, parentId);
            } else {
                throw new IllegalStateException("object does not exist "+index.writeAlias()+'/'+type +"/"+objectId);
            }
        }
    }

    @Override
    public Set<String> recentlyModifiedIds(String parentId) {
        List<String> list = recentlyModifiedIdsStack.list(redisKey(parentId));
        Set<String> result = new LinkedHashSet<>(list);
        return result;
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
    public BulkIndexingOperations bulkIndexer() {
        // use non threaded bulk indexer by default
        return client.bulkIndexer(index.writeAlias(), type, 100, 0);
    }
}

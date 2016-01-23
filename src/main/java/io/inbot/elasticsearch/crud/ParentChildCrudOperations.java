package io.inbot.elasticsearch.crud;

import com.codahale.metrics.MetricSet;
import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import java.util.Set;
import java.util.function.Function;

public interface ParentChildCrudOperations extends MetricSet {

    JsonObject create(JsonObject object, String parentId, boolean replace);

    JsonObject update(String objectId, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f, String parentId);

    JsonObject get(String id, String parentId);

    void delete(String objectId, String parentId);

    Set<String> recentlyModifiedIds(String parentId);

    PagedSearchResponse pagedSearch(JsonObject q, int size, int from);

    IterableSearchResponse iterableSearch(JsonObject q, int pageSize, int ttlMinutes, boolean rawResults);

    JsonObject searchUnique(JsonObject q);

    JsonObject get(boolean cached, String id, String parentId);

    /**
     * Use this to efficiently bulk index many objects in one go.
     * @return a bulkIndexer.
     */
    BulkIndexingOperations bulkIndexer();
}
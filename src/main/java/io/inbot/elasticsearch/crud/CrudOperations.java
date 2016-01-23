package io.inbot.elasticsearch.crud;

import com.codahale.metrics.MetricSet;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.IterableSearchResponse;
import io.inbot.elasticsearch.client.PagedSearchResponse;
import java.util.Set;
import java.util.function.Function;

public interface CrudOperations extends MetricSet {

    JsonObject create(JsonObject object, boolean replace);

    Set<String> recentlyModifiedIds();

    JsonObject update(String id, boolean modifyUpdatedAt, Function<JsonObject, JsonObject> f);

    JsonObject get(String id);

    JsonObject get(boolean cached, String id);

    JsonArray mget(boolean cached, String...ids);

    void delete(String id);

    void deleteByQuery(JsonObject query);

    PagedSearchResponse pagedSearch(JsonObject q, int size, int from);

    PagedSearchResponse pagedSearch(JsonObject q, int size, int from, String...fields);

    IterableSearchResponse iterableSearch(JsonObject q, int pageSize, int ttlMinutes, boolean rawResults);

    JsonObject searchUnique(JsonObject q);

    /**
     * Use this to efficiently bulk index many objects in one go.
     * @return a bulkIndexer.
     */
    BulkIndexingOperations bulkIndexer();


    JsonObject mapping();
}
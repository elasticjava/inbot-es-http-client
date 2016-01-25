package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.bulkindexing.BulkIndexer;
import java.io.IOException;
import org.apache.http.client.ClientProtocolException;

public interface EsAPIClient {

    boolean aliasExists(String alias);

    void backup(String indexName, String file);

    JsonObject bulkIndex(String index, String type, String finalBody) throws IOException, ClientProtocolException;

    /**
     * Use this if you are going to write multiple objects in one transaction.
     *
     * Note. you should use a try with resources or call close manually on the bulk indexer instance.
     * @param index index
     * @param type type
     * @param batchSize number of objects sent to es in one go
     * @param threads how many threads to use; be careful/gentle with this. Think in terms of es cpus dedicated to indexing.
     * @return bulk indexer
     */
    BulkIndexer bulkIndexer(String index, String type, int batchSize, int threads);

    public JsonObject count(String index, String type, JsonObject query);

    JsonObject createIndexMapping(String index, JsonObject mapping);

    JsonObject createIndexMappingFromResource(String index, String resource, int defaultNumberOfReplicas);

    JsonObject createObject(String index, String type, String id, String parentId, JsonObject object, boolean replaceExisting);

    void deleteByQuery(String index, String type, JsonObject query);

    boolean deleteIndex(String index);

    JsonObject deleteObject(String index, String type, String id);

    JsonObject deleteObject(String index, String type, String id, String version);

    JsonObject deleteObject(String index, String type, String id, String version, String parentId);
    JsonObject deletePercolator(String index, String id);
    JsonObject getMapping(String index, String type);

    JsonObject getObject(String index, String type, String id);
    JsonObject getObject(String index, String type, String id, String parentId);

    JsonObject getObjects(String index, String type, String...ids);

    JsonObject getPercolator(String index, String id);

    boolean indexExists(String indexName);

    JsonArray indicesFor(String alias);

    IterableSearchResponse iterableSearch(ElasticSearchType type, JsonObject q, int pageSize, int ttlMinutes, boolean rawResults);

    IterableSearchResponse iterableSearch(String index, String type, JsonObject q, int pageSize, int ttlMinutes, boolean rawResults);

    PagedSearchResponse pagedSearch(ElasticSearchType type, JsonObject q, int size, int from);

    PagedSearchResponse pagedSearch(String index, String type, JsonObject q, int size, int from);

    PagedSearchResponse pagedSearch(String index, String type, JsonObject q, int size, int from, String...fields);

    JsonObject percolate(String index, String type, JsonObject doc);

    JsonObject refresh();

    JsonObject refresh(String index);

    JsonObject refresh(String index, String type);

    JsonObject registerPercolator(String index, String id, JsonObject object);

    void restore(String indexName, String file);

    JsonObject search(String index, String type, JsonObject query);

    default JsonObject search(ElasticSearchType type, JsonObject query) {
        return search(type.readAlias(), type.type(), query);
    }

    void swapAlias(String alias, String newIndex);

    JsonObject updateObject(String index, String type, String id, String version, JsonObject object);

    JsonObject updateObject(String index, String type, String id, String parentId, String version, JsonObject object);

    void verbose(boolean on);

    void restore(ElasticSearchIndex index, String file);

    void backup(ElasticSearchIndex index, String file);

    void migrateIndex(ElasticSearchIndex index);

    void migrateIndex(ElasticSearchIndex index, int replicas);

    JsonObject getAliases();
}

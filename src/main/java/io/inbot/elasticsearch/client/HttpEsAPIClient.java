package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static io.inbot.elasticsearch.client.QueryBuilder.matchAll;
import static io.inbot.elasticsearch.client.QueryBuilder.queryWithVersion;

import com.github.jillesvangurp.urlbuilder.UrlBuilder;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.exceptions.JsonTypeMismatchException;
import com.github.jsonj.tools.JsonParser;
import com.google.common.collect.Iterables;
import io.inbot.datemath.DateMath;
import io.inbot.elasticsearch.bulkindexing.BulkIndexer;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.bulkindexing.LoggingStatusHandler;
import io.inbot.elasticsearch.exceptions.EsBadRequestException;
import io.inbot.elasticsearch.exceptions.EsNotFoundException;
import io.inbot.elasticsearch.jsonclient.JsonJRestClient;
import io.inbot.utils.IOUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpEsAPIClient implements EsAPIClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpEsAPIClient.class);
    private final JsonParser parser;
    private final JsonJRestClient jsonJRestClient;
    private final int maxPageSize;

    public HttpEsAPIClient(JsonJRestClient jsonJRestClient, JsonParser parser, int maxPageSize) {
        this.jsonJRestClient = jsonJRestClient;
        this.parser = parser;
        this.maxPageSize = maxPageSize;
    }

    @Override
    public boolean aliasExists(String alias) {
        return jsonJRestClient.get(alias).orElse(null) != null;
    }

    @Override
    public JsonObject getAliases() {
        return jsonJRestClient.get("/_aliases").get();
    }

    @Override
    public void backup(String indexName, String file) {
        try(BufferedWriter bw = IOUtils.gzipFileWriter(file)) {
            JsonObject q = queryWithVersion(matchAll());
            for(JsonObject doc: iterableSearch(indexName, null, q, 1000, 100, true)) {
                doc.serialize(bw);
                bw.append('\n');
            }
        } catch (IOException e) {
            throw new IllegalStateException("ERROR "+e.getMessage(),e);
        }
    }

    @Override
    public void backup(ElasticSearchIndex index, String file) {
        backup(index.readAlias(), file);
    }

    @Override
    public void migrateIndex(ElasticSearchIndex index) {
        migrateIndex(index, -1); // creates the index with the configured number of replicas on the cluster
    }

    @Override
    public void migrateIndex(ElasticSearchIndex index, int replicas) {
        String alias = index.aliasPrefix(); // the index name without the version or read/write aliases
        String mappingResource = index.mappingResource();

        if(isIndexMigrationNeeded(index)) {
            String newIndex = index.indexName();
            if(!aliasExists(alias) && indexExists(alias)) {
                // somebody managed to index stuff before we had a chance to create the index properly
                throw new IllegalStateException("an index already exists with the alias name " + alias);
            }
            if(!indexExists(newIndex)) {
                // no index exists of the specified version
                createIndexMappingFromResource(newIndex, mappingResource, replicas);
            }
            if(aliasExists(alias)) {
                JsonArray currentIndices = indicesFor(alias);
                if(currentIndices.size() > 1) {
                    // we don't support this currently
                    throw new IllegalArgumentException("cannot swap alias " + alias + " because it points to more than one index: " + currentIndices);
                } else if(currentIndices.size() == 1) {
                    if(!newIndex.equals(currentIndices.get(0).asString())) {

                        long startTime = System.currentTimeMillis();

                        // we need to reindex the old index into the new one in two steps
                        String oldIndex = currentIndices.get(0).asString();
                        boolean reindexSuccessfull = reindex(index, oldIndex, matchAll());
                        // now we can switch over the alias
                        JsonArray actions = array();

                        if(currentIndices.size() == 1 && !newIndex.equals(currentIndices.get(0).asString())) {
                            actions.add(object(field("remove", object(field("index", currentIndices.first()), field("alias", alias)))));
                        }

                        actions.add(object(field("add", object(field("index", newIndex), field("alias", alias)))));
                        jsonJRestClient.post(UrlBuilder.url("/").append("_aliases").build(), object(field("actions", actions)))
                                .orElseThrow(() -> new EsNotFoundException());

                        // reindex any documents that were added since we began the migration
                        JsonObject modifiedQuery = QueryBuilder.rangeQuery("updated_at", DateMath.formatIsoDate(startTime - 5000), true, null, true);
                        reindexSuccessfull = reindexSuccessfull && reindex(index, oldIndex, modifiedQuery);

                        // remove the old index
                        if(reindexSuccessfull) {
                            LOG.info("remove old index " + oldIndex);
                            deleteIndex(oldIndex);
                        } else {
                            LOG.error("there was an issue reindexing " + oldIndex + ". Not removing it and you should check it.");
                        }
                    } else {
                        LOG.warn("alias " + alias + " already points to " + newIndex);
                    }
                } else {
                    LOG.warn("alias exists but does not have any indices");
                }
            } else {
                LOG.info("alias does not exist " + alias + " create new index with alias");
                if(indexExists(alias)) {
                    throw new IllegalStateException("an index already exists with the alias name " + alias);
                }
                if(!indexExists(newIndex)) {
                    createIndexMappingFromResource(newIndex, mappingResource, replicas);
                }
                JsonArray actions = array();

                actions.add(object(field("add", object(field("index", newIndex), field("alias", alias)))));
                jsonJRestClient.post(UrlBuilder.url("/").append("_aliases").build(), object(field("actions", actions)))
                        .orElseThrow(() -> new EsNotFoundException());
            }
        } else {
            LOG.info("no migration needed for " + alias + " version " + index.version());
        }
    }

    private boolean reindex(ElasticSearchIndex index, String oldIndex, JsonObject q) {
        JsonObject query = queryWithVersion(q);
        // uses multiple threads to minimize downtime
        try(BulkIndexer bulkIndexer = bulkIndexer(index.indexName(), null, 1000, 9)) {
            LoggingStatusHandler statusHandler = new LoggingStatusHandler(LOG);
            bulkIndexer.setBulkIndexerStatusHandler(statusHandler);
            query.put("fields", array("_source","_parent","_id","_type"));
            for(JsonObject o: Iterables.concat(
                    iterableSearch(oldIndex, null, query, 1000, 20, true),
                    iterableSearch(oldIndex, ".percolator", query, 1000, 20, true)) // explicitly include percolator documents
                    ) {
                String parent = o.getString("fields","_parent");
                JsonObject object = o.getObject("_source");
                String type = o.getString("_type");
                object.put("_type", type);
                if(StringUtils.isNotEmpty(parent)) {
                    bulkIndexer.index(object,parent);
                } else {
                    bulkIndexer.index(object);
                }
            }
            if(!statusHandler.status().get("reindex_success", false)) {
                LOG.error("there were problems re-indexing " + index.indexName() + ": " +statusHandler.status());
                return false;
            } else {
                return true;
            }
        } catch (IOException e) {
            // this is bad, we should NOT delete the old index until we fix it
            LOG.error("SEVERE PROBLEM: can't reindex index " + oldIndex + " into " + index.indexName() + " because " + e.getMessage(), e);
            throw new IllegalStateException("SEVERE PROBLEM: can't reindex index " + oldIndex + " into " + index.indexName() + " because " + e.getMessage(), e);
        }
    }


    private boolean isIndexMigrationNeeded(ElasticSearchIndex index) {
        String alias = index.readAlias();
        if(!aliasExists(alias)) {
            return true;
        }
        JsonArray indices = indicesFor(alias);
        if(indices.size() > 1) {
            throw new IllegalStateException("more than one index for alias " + alias);
        }
        if(indices.size() == 1) {
            if(indices.first().asString().endsWith("_v"+index.version())) {
                // the alias exists for a single index with the correct suffix
                return false;
            }
        }
        // if the alias does not exist, it will need to be created
        return true;
    }

    private Supplier<EsNotFoundException> notFoundSupplier() {
        Supplier<EsNotFoundException> notFound = () -> new EsNotFoundException();
        return notFound;
    }

    @Override
    public JsonObject bulkIndex(String index, String type, String bulkBody) throws IOException, ClientProtocolException {
        return jsonJRestClient.put(UrlBuilder.url("/").append(index, type, "_bulk").build(), bulkBody).orElseThrow(notFoundSupplier());
    }

    @Override
    public BulkIndexer bulkIndexer(String index, String type, int batchSize, int threads) {
        Validate.isTrue(batchSize < maxPageSize, "pageSize should be less than " + maxPageSize);
        return new BulkIndexer(this, index, type, batchSize, threads, false);
    }

    @Override
    @Deprecated // use search with size=0 and grab the size
    public JsonObject count(String index, String type, JsonObject query) {
        String url = UrlBuilder.url("/")
                .append(index, type, "_count")
                .build();
        query.remove("version");
        query.remove("size");
        return jsonJRestClient.get(url, query.toString()).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject createIndexMapping(String index, JsonObject mapping) {
        return jsonJRestClient.put(index,mapping).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject createIndexMappingFromResource(String index, String resource, int defaultNumberOfReplicas) {
        try {
            JsonObject mapping = parser.parse(IOUtils.resource(resource)).asObject();
            JsonObject indexSettings = mapping.getOrCreateObject("settings","index");
            if(!indexSettings.containsKey("number_of_replicas") && defaultNumberOfReplicas >=0) {
                indexSettings.put("number_of_replicas", defaultNumberOfReplicas);
            }
            return createIndexMapping(index, mapping);
        } catch (IOException e) {
            throw new IllegalStateException("error reading mapping " + resource);
        }
    }

    @Override
    public JsonObject createObject(String index, String type, String id, String parentId, JsonObject object, boolean replaceExisting) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.notNull(object);
        UrlBuilder urlBuilder=UrlBuilder.url("/");
        if (StringUtils.isNotEmpty(parentId)) {
            urlBuilder.append(index, type, id).queryParam("parent", parentId);
        } else {
            urlBuilder.append(index, type, id);
        }
        if (!replaceExisting) {
            // this will cause elastic search to throw a version conflict if an object with the same id already exists
            urlBuilder.queryParam("op_type", "create");
        }
        String url = urlBuilder.build();
        return jsonJRestClient.post(url, object).orElseThrow(notFoundSupplier());
    }

    @Override
    public void deleteByQuery(String index, String type, JsonObject query) {
        try(BulkIndexer bulkIndexer = bulkIndexer(index, type, 1000, 1)) {
            for(JsonObject hit:iterableSearch(index, type, query, 1000, 100, true)) {
                bulkIndexer.delete(hit.getString("_id"));
            }

        } catch (IOException e) {
            throw new IllegalStateException("error bulk deleting", e);
        }
    }

    @Override
    public boolean deleteIndex(String index) {
        return jsonJRestClient.delete(UrlBuilder.url("/").append(index).build()).orElse(null) != null;
    }

    @Override
    public JsonObject deleteObject(String index, String type, String id) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.isTrue(StringUtils.isNotBlank(id));
        String url = UrlBuilder.url("/").append(index, type, id).build();
        return jsonJRestClient.delete(url).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject deleteObject(String index, String type, String id, String version) {
        return deleteObject(index, type, id, version, null);
    }

    @Override
    public JsonObject deleteObject(String index, String type, String id, String version, String parentId) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.isTrue(StringUtils.isNotBlank(id));

        UrlBuilder urlBuilder = UrlBuilder.url("/").append(index, type, id);
        if (StringUtils.isNotBlank(version)) {
            urlBuilder.queryParam("version", version);
        }
        if (StringUtils.isNotEmpty(parentId)) {
            urlBuilder.queryParam("parent", parentId);
        }
        String url = urlBuilder.build();
        return jsonJRestClient.delete(url).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject deletePercolator(String index, String id) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(id));
        UrlBuilder urlBuilder;
        urlBuilder = UrlBuilder.url("/").append(index, ".percolator", id);

        String url = urlBuilder.build();
        return jsonJRestClient.delete(url).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject getMapping(String index, String type) {
        return jsonJRestClient.get(UrlBuilder.url("/").append(index,type,"_mapping").build()).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject getObject(String index, String type, String id) {
        return getObject(index, type, id, null);
    }

    @Override
    public JsonObject getObject(String index, String type, String id, String parentId) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.isTrue(StringUtils.isNotBlank(id));
        String url;
        if (StringUtils.isNotEmpty(parentId)) {
            url = UrlBuilder.url("/").append(index, type, id).queryParam("parent", parentId).build();
        } else {
            url = UrlBuilder.url("/").append(index, type, id).build();
        }
        return jsonJRestClient.get(url).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject getObjects(String index, String type, String... ids) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.isTrue(ids.length > 0);
        JsonObject idsQuery = object(field("ids", array(ids)));
        return jsonJRestClient.get(UrlBuilder.url("/").append(index, type, "_mget").build(), idsQuery).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject getPercolator(String index, String id) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(id));
        UrlBuilder urlBuilder;
        urlBuilder = UrlBuilder.url("/").append(index, ".percolator", id);

        String url = urlBuilder.build();
        return jsonJRestClient.get(url).orElseThrow(notFoundSupplier());
    }

    @Override
    public boolean indexExists(String indexName) {
        return jsonJRestClient.get(UrlBuilder.url("/").append(indexName, "_settings").build()).orElse(null) != null;
    }

    @Override
    public JsonArray indicesFor(String alias) {
        JsonArray indexNames = array();
        JsonObject parsed = jsonJRestClient.get(UrlBuilder.url("/").append("_alias", alias).build())
                .orElseThrow(() -> new IllegalArgumentException("no such alias" + alias));
        for (Entry<String, JsonElement> e : parsed.entrySet()) {
            indexNames.add(e.getKey());
        }

        return indexNames;
    }

    @Override
    public IterableSearchResponse iterableSearch(ElasticSearchType type, JsonObject q, int pageSize, int ttlMinutes, boolean rawResults) {
        return iterableSearch(type.readAlias(), type.type(), q, pageSize, ttlMinutes, rawResults);
    }

    @Override
    public IterableSearchResponse iterableSearch(final String index, final String type, final JsonObject q, int pageSize, int ttlMinutes, boolean rawResults) {
        Validate.isTrue(pageSize < maxPageSize, "pageSize should be less than " + maxPageSize);

        final String ttl = "" + ttlMinutes + "m";
        UrlBuilder builder = UrlBuilder.url("/").append(index, type, "_search").queryParam("search_type", "scan").queryParam("scroll", ttl).queryParam("size", pageSize);
        JsonElement parent = q.remove("parent");
        q.put("version", true);
        if (parent != null) {
            builder.queryParam("parent", parent.asString());
        }
        String searchUrl = builder.build();
        JsonObject results;
        try {
            results = jsonJRestClient.get(searchUrl, q).orElseThrow(notFoundSupplier());
        } catch (EsBadRequestException e) {
            JsonObject originalMessage = parser.parseObject(e.getMessage());
            originalMessage.put("query", q);
            originalMessage.put("index", index);
            originalMessage.put("type", type);
            throw new EsBadRequestException(originalMessage);
        }
        final String scrollId = results.getString("_scroll_id");

        return new IterableSearchResponse(results.getInt("hits", "total"), new Iterator<JsonObject>() {
            JsonObject next = null;
            String nextScrollId = scrollId;
            JsonArray page = null;
            int rs = 0;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                } else {
                    if (page == null) {
                        String nextUrl = UrlBuilder.url("/").append("_search", "scroll").queryParam("scroll", ttl).build();
                        JsonObject scrollPage = jsonJRestClient.get(nextUrl, nextScrollId).orElseThrow(notFoundSupplier());
                        page = scrollPage.getArray("hits", "hits");
                        nextScrollId = scrollPage.getString("_scroll_id");

                        rs = 0;
                    }
                    if (page != null && page.size() == 0) {
                        return false;
                    } else if (page != null) {
                        JsonObject item = page.get(rs++).asObject();
                        if (rawResults) {
                            next = item;
                        } else {
                            next = item.getObject("_source");
                            next.put("_type", item.getString("_type"));
                            next.put("_version", item.getString("_version"));
                            try {
                                next.put("es_search_score", item.get("_score", 0.0));
                            } catch (JsonTypeMismatchException e) {
                                // es returns null score sometimes instead of omitting it
                            }
                        }
                        if (rs >= page.size()) {
                            page = null;
                        }
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public JsonObject next() {
                if (hasNext()) {
                    JsonObject o = next;
                    next = null;
                    return o;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Override
    public PagedSearchResponse pagedSearch(ElasticSearchType type, JsonObject q, int size, int from) {
        return pagedSearch(type.readAlias(), type.type(), q, size, from);
    }

    @Override
    public PagedSearchResponse pagedSearch(String index, String type, JsonObject q, int pageSize, int from) {
        Validate.isTrue(pageSize < maxPageSize, "pageSize should be less than " + maxPageSize);
        try {
            // overwrites whatever you set in the query yourself!
            q.put("size", pageSize);
            q.put("from", from);
            q.put("version", true);
            UrlBuilder builder = UrlBuilder.url("/").append(index, type, "_search");
            JsonElement parent = q.remove("parent");
            if (parent != null) {
                builder.queryParam("parent", parent.asString());
            }
            String searchUrl = builder.build();
            JsonObject parsed = jsonJRestClient.get(searchUrl, q).orElseThrow(notFoundSupplier());
            return new EsSearchResponse(parsed, pageSize, from);
        } catch (EsBadRequestException e) {
            JsonObject originalMessage = parser.parseObject(e.getMessage());
            originalMessage.put("query", q);
            originalMessage.put("index", index);
            originalMessage.put("type", type);
            originalMessage.put("pageSize", pageSize);
            originalMessage.put("from", from);
            throw new EsBadRequestException(originalMessage);
        }
    }

    @Override
    public PagedSearchResponse pagedSearch(String index, String type, JsonObject q, int pageSize, int from, String...fields) {
        q.put("fields", array(fields));
        return pagedSearch(index, type, q,pageSize, from);
    }

    @Override
    public JsonObject percolate(String index, String type, JsonObject doc) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.notNull(doc);

        UrlBuilder urlBuilder;
        urlBuilder = UrlBuilder.url("/").append(index, type, "_percolate");
        String url = urlBuilder.build();

        return jsonJRestClient.get(url, doc).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject refresh() {
        return refresh(null, null);
    }
    @Override
    public JsonObject refresh(String index) {
        return refresh(index, null);
    }

    @Override
    public JsonObject refresh(String index, String type) {
        return jsonJRestClient.post(UrlBuilder.url("/").append(index, type, "_refresh").build()).orElseThrow(notFoundSupplier());
    }

    @Override
    public JsonObject registerPercolator(String index, String id, JsonObject object) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(id));
        UrlBuilder urlBuilder;
        urlBuilder = UrlBuilder.url("/").append(index, ".percolator", id);

        String url = urlBuilder.build();
        return jsonJRestClient.put(url, object).orElseThrow(notFoundSupplier());
    }

    @Override
    public void restore(String indexName, String file) {
        // make sure the index exists with the right mapping before restoring
        try(BufferedReader br = IOUtils.gzipFileReader(file)) {
            try(BulkIndexingOperations indexer = bulkIndexer(indexName, null, 100, 1)) {
                br.lines().forEach(line -> {
                    JsonObject o = parser.parseObject(line);
                    String id = o.getString("_id");
                    String type = o.getString("_type");
                    String parent = o.getString("fields","_parent");
                    JsonObject source = o.getObject("_source");
                    indexer.index(id,type,parent,null,source);
                });
            }

        } catch (IOException e) {
            throw new IllegalStateException("ERROR "+e.getMessage(),e);
        }
    }

    @Override
    public void restore(ElasticSearchIndex index, String file) {
        restore(index.writeAlias(),file);
    }

    @Override
    public JsonObject search(String index, String type, JsonObject query) {
        String url = UrlBuilder.url("/")
                .append(index, type, "_search")
                .build();
        return jsonJRestClient.get(url, query).orElseThrow(notFoundSupplier());
    }

    @Override
    public void swapAlias(String alias, String newIndex) {
        if (!indexExists(newIndex)) {
            throw new IllegalArgumentException("index " + newIndex + " does not exist");
        }
        JsonArray currentIndices = indicesFor(alias);
        if (currentIndices.size() > 1) {
            throw new IllegalArgumentException("cannot swap alias " + alias + " because it points to more than one index: " + currentIndices);
        }
        JsonArray actions = array();

        if (currentIndices.size() == 1 && !newIndex.equals(currentIndices.get(0).asString())) {
            actions.add(object(field("remove", object(field("index", currentIndices.first()), field("alias", alias)))));
        }
        actions.add(object(field("add", object(field("index", newIndex), field("alias", alias)))));
        jsonJRestClient.post(UrlBuilder.url("/").append("_aliases").build(), object(field("actions", actions))).orElseThrow(notFoundSupplier());
        if (currentIndices.size() == 1 && !newIndex.equals(currentIndices.get(0).asString())) {
            // remove the old index
            deleteIndex(currentIndices.first().asString());
        }
    }

    @Override
    public JsonObject updateObject(String index, String type, String id, String version, JsonObject object) {
        return updateObject(index, type, id, null, version, object);
    }

    @Override
    public JsonObject updateObject(String index, String type, String id, String parentId, String version, JsonObject object) {
        Validate.isTrue(StringUtils.isNotBlank(index));
        Validate.isTrue(StringUtils.isNotBlank(type));
        Validate.isTrue(StringUtils.isNotBlank(id));
        Validate.isTrue(StringUtils.isNotBlank(version));
        Validate.notNull(object);
        String url;
        if (StringUtils.isNotEmpty(parentId)) {
            url = UrlBuilder.url("/").append(index, type, id).queryParam("version", version).queryParam("parent", parentId).build();
        } else {
            url = UrlBuilder.url("/").append(index, type, id).queryParam("version", version).build();
        }

        return jsonJRestClient.put(url, object).orElseThrow(notFoundSupplier());
    }

    @Override
    public void verbose(boolean on) {
        jsonJRestClient.setVerbose(on);
    }
}
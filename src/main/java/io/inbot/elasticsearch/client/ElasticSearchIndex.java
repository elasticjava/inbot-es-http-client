package io.inbot.elasticsearch.client;

public interface ElasticSearchIndex {

    public static ElasticSearchIndex create(String alias, int version, String mappingResource) {
        return new SimpleIndex(alias,version,mappingResource);
    }
    String mappingResource();

    String indexName();

    String aliasPrefix();

    int version();

    String readAlias();

    String writeAlias();
}
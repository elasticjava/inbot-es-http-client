package io.inbot.elasticsearch.client;

public interface ElasticSearchIndex {

    String mappingResource();

    String indexName();

    String aliasPrefix();

    int version();

    String readAlias();

    String writeAlias();

    ElasticsearchType[] types();

}
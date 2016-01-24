package io.inbot.elasticsearch.client;

public final class SimpleIndex implements ElasticSearchIndex {
    private final String alias;
    private final int version;
    private final String mappingResource;

    public SimpleIndex(String alias, int version, String mappingResource) {
        this.alias = alias;
        this.version = version;
        this.mappingResource = mappingResource;
    }

    @Override
    public String writeAlias() {
        return alias;
    }

    @Override
    public int version() {
        return version;
    }

    @Override
    public String readAlias() {
        return alias;
    }

    @Override
    public String mappingResource() {
        return mappingResource;
    }

    @Override
    public String indexName() {
        return alias +"_v"+version;
    }

    @Override
    public String aliasPrefix() {
        return alias;
    }
}
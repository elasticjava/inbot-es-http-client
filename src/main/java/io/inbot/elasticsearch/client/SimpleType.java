package io.inbot.elasticsearch.client;

public class SimpleType implements ElasticSearchType {
    private final ElasticSearchIndex index;
    private final String typeName;
    private final boolean hasParent;

    public SimpleType(ElasticSearchIndex index, String typeName, boolean hasParent) {
        this.index = index;
        this.typeName = typeName;
        this.hasParent = hasParent;
    }
    @Override
    public ElasticSearchIndex index() {
        return index;
    }

    @Override
    public boolean parentChild() {
        return hasParent;
    }

    @Override
    public String type() {
        return typeName;
    }
}
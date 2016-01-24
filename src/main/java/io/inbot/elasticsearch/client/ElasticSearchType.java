package io.inbot.elasticsearch.client;

import java.util.Locale;

/**
 * Parent type for the various type enums. Each enum implementation represents the types for a single index.
 */
public interface ElasticSearchType extends ElasticSearchIndex {

    public static ElasticSearchType create(ElasticSearchIndex index, String name) {
        return new SimpleType(index, name, false);
    }

    ElasticSearchIndex index();

    @Override
    default String readAlias() {
        return index().readAlias();
    }

    @Override
    default String writeAlias() {
        return index().writeAlias();
    }

    default String type() {
        // useful implementation if you use an enum to list your types
        return this.toString().toLowerCase(Locale.ENGLISH); // es doesn't like uppercase types
    }

    @Override
    default int version() {
        return index().version();
    }

    default boolean matches(ElasticSearchIndex index, String type) {
        return index().equals(index) && type().equals(type);
    }

    default boolean matches(ElasticSearchIndex index, String type, int version) {
        return index().equals(index) && type().equals(type) && index().version() <= version;
    }

    default boolean parentChild() {
        return false;
    }

    @Override
    default String mappingResource() {
        return index().mappingResource();
    }
    @Override
    default String indexName() {
        return index().indexName();
    }
    @Override
    default String aliasPrefix() {
        return index().aliasPrefix();
    }
}
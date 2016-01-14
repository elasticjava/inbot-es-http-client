package io.inbot.elasticsearch.client;

import java.util.Locale;

/**
 * Parent type for the various type enums. Each enum implementation represents the types for a single index.
 */
public interface ElasticsearchType {
    ElasticSearchIndex index();

    default String readAlias() {
        return index().readAlias();
    }

    default String writeAlias() {
        return index().writeAlias();
    }

    default String type() {
        return this.toString().toLowerCase(Locale.ENGLISH); // es doesn't like uppercase types
    }

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
}
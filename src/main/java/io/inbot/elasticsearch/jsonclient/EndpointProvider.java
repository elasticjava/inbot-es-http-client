package io.inbot.elasticsearch.jsonclient;

/**
 * Interfaces for dealing with service endpoints
 *
 */
public interface EndpointProvider {

    /**
     * @return a usable endpoint to connect to elasticsearch. E.g. http://localhost:9200
     */
    String endPoint();

    /**
     * Called by JsonJEsRestClient when it can't connect to elasticsearch. Use this information to e.g. fall back to an alternative endpoint
     * or throw an exception.
     * @param url the endpoint it tried to connect.
     */
    default void failEndpoint(String url) {
        // maybe do something meaningful
    }
}

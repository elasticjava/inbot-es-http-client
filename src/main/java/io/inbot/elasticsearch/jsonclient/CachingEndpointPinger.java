package io.inbot.elasticsearch.jsonclient;

import com.google.common.cache.CacheLoader;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

public final class CachingEndpointPinger extends CacheLoader<String, Boolean> {

    private final HttpClient httpClient;
    public CachingEndpointPinger(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Boolean load(String endpoint) throws Exception {
        try {
            return httpClient.execute(new HttpGet(endpoint), response -> response.getStatusLine().getStatusCode() == 200);
        } catch (Throwable t) {
            return false;
        }
    }
}
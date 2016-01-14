package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonObject;

public interface PagedSearchResponse extends SearchResponse, Paging {
    JsonObject get(int i);

    JsonObject getFirstResult();

}

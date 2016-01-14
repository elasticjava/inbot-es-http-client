package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import java.util.Iterator;

public class SimpleSearchResponse implements PagedSearchResponse {
    private final JsonArray objects;

    public SimpleSearchResponse(JsonArray objects) {
        this.objects = objects;
    }

    @Override
    public int size() {
        return objects.size();
    }

    @Override
    public boolean page() {
        return true;
    }

    @Override
    public PagedSearchResponse getAsPagedResponse() {
        return this;
    }

    @Override
    public Iterator<JsonObject> iterator() {
        return objects.objects().iterator();
    }

    @Override
    public int pageSize() {
        return objects.size();
    }

    @Override
    public boolean hasPreviousResults() {
        return false;
    }

    @Override
    public int previousFrom() {
        return 0;
    }

    @Override
    public int from() {
        return 0;
    }

    @Override
    public boolean hasMoreResults() {
        return false;
    }

    @Override
    public int nextFrom() {
        return 0;
    }

    @Override
    public JsonObject get(int i) {
        return objects.get(i).asObject();
    }

    @Override
    public JsonObject getFirstResult() {
        return objects.get(0).asObject();
    }
}

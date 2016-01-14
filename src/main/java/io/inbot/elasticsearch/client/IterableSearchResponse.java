package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonObject;
import java.util.Iterator;
import java.util.function.Function;

public class IterableSearchResponse implements SearchResponse {
    private final Iterator<JsonObject> results;
    private final int size;

    public IterableSearchResponse(int size, Iterator<JsonObject> results) {
        this.size = size;
        this.results = results;
    }

    public IterableSearchResponse(int size, Iterable<JsonObject> results) {
        this.size = size;
        this.results = results.iterator();
    }

    @Override
    public ProcessingSearchResponse map(Function<JsonObject, JsonObject> f) {
        return ProcessingSearchResponse.map(this, f);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<JsonObject> iterator() {
        return results;
    }

    @Override
    public boolean page() {
        return false;
    }

    @Override
    public PagedSearchResponse getAsPagedResponse() {
        throw new UnsupportedOperationException("not a paged response");
    }
}
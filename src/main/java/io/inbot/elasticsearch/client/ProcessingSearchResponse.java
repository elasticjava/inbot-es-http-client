package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonObject;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Wraps another search response and allows you to process the individual results as they are iterated through.
 */
public class ProcessingSearchResponse implements SearchResponse {
    private final SearchResponse response;
    private final Function<JsonObject, JsonObject> processor;

    private ProcessingSearchResponse(SearchResponse response, Function<JsonObject, JsonObject> processor) {
        this.response = response;
        this.processor = processor;
    }

    public static ProcessingSearchResponse map(SearchResponse response, Function<JsonObject, JsonObject> f) {
        return new ProcessingSearchResponse(response, f);
    }

    @Override
    public ProcessingSearchResponse map(Function<JsonObject, JsonObject> f) {
        return new ProcessingSearchResponse(this, f);
    }

    @Override
    public Iterator<JsonObject> iterator() {
        return StreamSupport.stream(response.spliterator(), false).map(processor).filter(e -> e!=null).iterator();
    }

    @Override
    public int size() {
        return response.size();
    }

    @Override
    public boolean page() {
        return response.page();
    }

    @Override
    public PagedSearchResponse getAsPagedResponse() {
        return response.getAsPagedResponse();
    }
}

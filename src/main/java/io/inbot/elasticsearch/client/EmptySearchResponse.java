package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;

import com.github.jsonj.JsonObject;
import java.util.Iterator;

public class EmptySearchResponse implements PagedSearchResponse {
    private final PagedSearchResponse emptyPage = new EsSearchResponse(object(field("hits",object(field("total",0)))), 0, 0);

    @Override
    public Iterator<JsonObject> iterator() {
        return emptyPage.iterator();
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean page() {
        return true;
    }

    @Override
    public PagedSearchResponse getAsPagedResponse() {
        return emptyPage;
    }

    @Override
    public int pageSize() {
        return 0;
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
        throw new IllegalStateException("no results");
    }

    @Override
    public JsonObject getFirstResult() {
        throw new IllegalStateException("no results");
    }
}
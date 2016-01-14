package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SimplePagedSearchResults implements PagedSearchResponse {

    private final Collection<JsonElement> objects;
    private final int size;
    private final int pageSize;
    private final int from;

    public SimplePagedSearchResults(Collection<JsonElement> objects, int size, int pageSize, int from) {
        this.objects = objects;
        this.size = size;
        this.pageSize = pageSize;
        this.from = from;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public int from() {
        return from;
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
        return objects.stream().map(e -> e.asObject()).iterator();
    }


    @Override
    public JsonObject get(int index) {
        if(index < 0 || index>objects.size()-1) {
            // fail fast
            throw new NoSuchElementException();
        }
        int i=0;
        Iterator<JsonObject> iterator = objects.stream().map(e -> e.asObject()).iterator();
        while(i < index && iterator.hasNext()) {
            iterator.next();
            i++;
        }
        // will throw noSuchElementException if out of range
        return iterator.next();
    }

    @Override
    public JsonObject getFirstResult() {
        return get(0);
    }

}

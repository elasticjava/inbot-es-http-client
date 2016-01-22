package io.inbot.elasticsearch.client;

import java.util.NoSuchElementException;

public interface Paging {
    int size();
    int pageSize();
    int from();

    default boolean hasPreviousResults() {
        return from() > 0;
    }

    default boolean hasMoreResults() {
        return from() + pageSize() < size();
    }

    default int previousFrom() {
        int previous = from() -pageSize();
        if(previous < 0) {
            previous = 0;
        }
        return previous;
    }

    default int nextFrom() {
        if(hasMoreResults()) {
            return from() + pageSize();
        } else {
            throw new NoSuchElementException("there are no more search results");
        }
    }
}

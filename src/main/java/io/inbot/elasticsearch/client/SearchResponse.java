package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.array;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface SearchResponse extends Iterable<JsonObject> {
    int size();

    boolean page();

    PagedSearchResponse getAsPagedResponse();

    default Stream<JsonObject> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public static SearchResponse empty() {
        return new EmptySearchResponse();
    }

    public static SearchResponse from(Collection<JsonElement> os) {
        return new IterableSearchResponse(os.size(), os.stream().map(e -> e.asObject()).iterator());
    }

    default SearchResponse map(Function<JsonObject, JsonObject> f) {
        return ProcessingSearchResponse.map(this, f);
    }

//    default String etag(String ifNoneMatch) {
//        String etag=null;
//        if(page()) {
//            // calculate the etag
//            Md5Stream md5Stream;
//            try {
//                md5Stream = new Md5Stream();
//                try {
//                    for(JsonObject result: this) {
//                        result.serialize(md5Stream);
//                    }
//                } finally {
//                    md5Stream.close();
//                }
//            } catch (IOException e) {
//                throw new IllegalStateException("unexpected exception" + ": " + e.getMessage(),e);
//            }
//            etag = md5Stream.etag();
//            if(StringUtils.isNotEmpty(ifNoneMatch)) {
//                if(etag.equals(ifNoneMatch)) {
//                    throw new NotModifiedException();
//                }
//            }
//        }
//        return etag;
//    }

    default String prettyPrint() {
        if(page()) {
            JsonArray results = array();
            for(JsonObject o: this) {
                results.add(o);
            }
            return results.prettyPrint();
        } else {
            throw new UnsupportedOperationException("cannot call on non paged search results");
        }
    }
}

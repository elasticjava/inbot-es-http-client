package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.array;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.StreamSupport;

public class EsSearchResponse implements PagedSearchResponse {
    private final JsonObject elasticSearchResponse;
    private final int from;
    private final int pageSize;

    public EsSearchResponse(JsonObject elasticSearchResponse, int pageSize, int from) {
        this.elasticSearchResponse = elasticSearchResponse;
        this.from = from;
        this.pageSize = pageSize;
    }

    @Override
    public ProcessingSearchResponse map(Function<JsonObject, JsonObject> f) {
        return ProcessingSearchResponse.map(this, f);
    }

    @Override
    public Iterator<JsonObject> iterator() {
        if(elasticSearchResponse == null) {
            throw new NoSuchElementException();
        }
        JsonArray hits = elasticSearchResponse.getArray("hits","hits");
        if(hits == null) {
            return array().objects().iterator();
        }
        return StreamSupport.stream(hits.objects().spliterator(), false).map(input -> {
            // ensure we can iterate multiple times by cloning, essential for e.g. etags
            JsonObject clone = input.deepClone();
            JsonObject hit = clone.asObject();
            JsonObject object = hit.getObject("_source");
            if(object==null) {
                object=hit;
            }
            object.put("_version", hit.getString("_version"));
            // when sorting es does not return a score. So assign 1 as default
//            try {
//                object.put("es_search_score", hit.get("_score", 1.0));
//            } catch (JsonTypeMismatchException e) {
//                // es returns null score sometimes instead of omitting it
//            }
            return object;

        }).iterator();
    }

    @Override
    public JsonObject get(int index) {
        if(elasticSearchResponse == null) {
            throw new NoSuchElementException();
        }
        JsonArray hits = elasticSearchResponse.getArray("hits","hits");
        JsonObject hit = hits.get(index).asObject();
        JsonObject object = hit.getObject("_source");
        object.put("_version", hit.getString("_version"));
        return object;
    }

    @Override
    public JsonObject getFirstResult() {
        if(size() > 1) {
            return get(0);
        } else if(size() == 0) {
            return null;
        } else {
            return get(0);
        }
    }

    @Override
    public String toString(){
    	StringBuffer bfr = new StringBuffer();
    	bfr.append("[");
    	int current = 0;
    	if(size() >0) {
            for (JsonObject hit : this) {
            	bfr.append(hit);
            	if (current < (size()-1)){
                	bfr.append(",");
            	}
            	current++;
            }
        }
    	bfr.append("]");
    	return bfr.toString();
    }

    @Override
    public int size() {
        if(elasticSearchResponse == null) {
            return 0;
        } else {
            return elasticSearchResponse.getInt("hits","total");
        }
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
    public PagedSearchResponse getAsPagedResponse() {
        return this;
    }

    @Override
    public boolean page() {
        return true;
    }
}

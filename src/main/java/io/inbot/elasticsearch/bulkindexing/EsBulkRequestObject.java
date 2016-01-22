package io.inbot.elasticsearch.bulkindexing;

import com.github.jsonj.JsonObject;
import java.util.function.Function;

public class EsBulkRequestObject {

    public final JsonObject metadata;
    public final JsonObject object;
    public final Function<JsonObject, JsonObject> transformFunction; // used during update


    public EsBulkRequestObject(JsonObject metadata, JsonObject object, Function<JsonObject,JsonObject> transformFunction) {
        this.metadata = metadata;
        this.object = object;
        this.transformFunction = transformFunction;
    }

    public boolean isSameVersion(String id, String version) {
        String mid = metadata.getString("index","_id");
        String mv = metadata.getString("index","index","_version");
        return id != null && version != null && id.equals(mid) && version.equals(mv);
    }

    @Override
    public String toString() {
        if(object != null) {
            return metadata.toString() + '\n' + object.toString() + '\n';
        } else {
            return metadata.toString() + '\n'; // deletes have no object
        }
    }

}

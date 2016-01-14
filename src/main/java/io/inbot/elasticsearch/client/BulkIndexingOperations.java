package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonObject;
import java.io.Closeable;
import java.util.function.Function;

/**
 * BulkIndexingOperations group the commonly used operations that you can execute on elasticsearch. Implementing classes are expected to pre-configure things
 * like the type and index via e.g. the constructor.
 *
 */
public interface BulkIndexingOperations extends Closeable {

    /**
     * Add an object to the index.
     * @param o the object
     */
    void index(JsonObject o);

    /**
     * Add an object of a type that has parent child relations enabled to the index.
     *
     * @param o the object
     * @param parentId the parent id of the object.
     */
    void index(JsonObject o, String parentId);

    /**
     * Add an object to the index and specify everything explicitly.
     * @param id of the object. If null, elasticsearch will generate a new id. Note, this is faster since technically specifying an id turns this into an upsert.
     * @param type type of the object
     * @param parentId the parent of the object if applicable
     * @param version version of the object
     * @param object the object
     */
    void index(String id, String type, String parentId, String version, JsonObject object);

    /**
     * Update an object already in elasticsearch.
     * @param id of the object
     * @param version the version of the object
     * @param parentId the parent of the object if applicable
     * @param oldObject the old object; needed because we don't want to get it during bulk updating unless we have to (e.g. version mismatch)
     * @param transformFunction the function that transforms the oldObject; may be applied to newer version of the object in case of version mismatch
     */
    void update(String id, String version, String parentId, JsonObject oldObject, Function<JsonObject, JsonObject> transformFunction);


    /**
     * Flush currently queued bulik indexing operations to elasticsearch. Usually there is no need to call this manually.
     */
    void flush();

    /**
     * Configure a handler to handle the different outcomes for each operation. This may be used to log success per operation, handle version conflicts or log
     * errors. Note, the call back opererations are per bulk operation and won't happen until the operation is flushed to elasticsearch.
     * @param statusHandler the custom handler object.
     */
    void setBulkIndexerStatusHandler(BulkIndexerStatusHandler statusHandler);

    /**
     * @param b when true, elasticsearch will refresh the index after each flush. When false, there may be significant delays between when you send a bulk
     * operation is sent to elasticsearch and the moment when it actually processes the operation.
     */
    void setRefresh(boolean b);

    /**
     * Delete an object  by id.
     * @param id object id.
     */
    void delete(String id);

    /**
     * Delete an object with a parent by id.
     * @param id object id.
     * @param parentId parent id of the object
     */
    void delete(String id, String parentId);

}
package io.inbot.elasticsearch.bulkindexing;

import com.github.jsonj.JsonObject;
import java.util.function.Function;

/**
 * The BulkIndexingOperations provide what are essentially asynchronous operations on an index to o insert, upsert, update, or delete objects. Operations are
 * grouped and periodically flushed to elasticsearch via its bulk indexing API. The response of this API includes detailed information on  what happened with
 * each operation. To facilitate processing this information we use this callback API.
 */
public interface BulkIndexerStatusHandler {

    /**
     * Called when specific items cannot be processed
     * @param code an errorcode indicating the nature of the problem
     * @param details details provided by elasticsearch about the problem
     */
    default void error(String code, JsonObject details) {
    }

    /**
     * There was a version conflict while we were trying to update the object
     * @param id id of the object with a conflict
     * @param updateFunction the update function that was specified for the object; you might try reapplying it to a more recent version of the object
     */
    default void handleVersionConflict(String id, Function<JsonObject,JsonObject> updateFunction) {
    }

    /**
     * The item was processed succesffuly.
     * @param item the item object in the elasticsearch response.
     */
    default void ok(JsonObject item) {
    }

    /**
     * @return status object; typically called after the bulk indexer has been closed. You can use this to return some statistics.
     */
    JsonObject status();

    /**
     * Called when the bulk elasticsearch index API call returns abnormally.
     * @param description describes what went wrong as well as possible
     */
    default void fail(String description) {
    }

    /**
     * Called when the bulk indexer has been initialized. Useful for e.g. logging this event.
     */
    default void start() {
    }

    /**
     * Called after the bulk indexe has been closed. Useful for e.g. logging this event.
     */
    default void done() {
    }

    /**
     * called when the bulk indexed 'flushes' to elasticsearch
     */
    default void flush() {
    }
}
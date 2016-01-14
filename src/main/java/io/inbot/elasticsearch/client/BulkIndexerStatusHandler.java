package io.inbot.elasticsearch.client;

import com.github.jsonj.JsonObject;
import java.util.function.Function;

/**
 * The BulkIndexingOperations provide what are essentially asynchronous operations on an index to o insert, upsert, update, or delete objects. Operations are
 * grouped and periodically flushed to elasticsearch via its bulk indexing API. The response of this API includes detailed information on  what happened with
 * each operation. To facilitate processing this information we use this callback API.
 *
 *  TODO cleanup API, use enum for error codes, all operations should report index, type, and id of the item that failed.
 */
public interface BulkIndexerStatusHandler {

    /**
     * Called when specific items cannot be processed
     * @param code an errorcode indicating the nature of the problem
     * @param details details provided by elasticsearch about the problem
     */
    default void error(String code, JsonObject details) {
    }

    default void handleVersionConflict(String id, Function<JsonObject,JsonObject> updateFunction) {
    }

    /**
     * The item was processed succesffuly.
     * @param item the item object in the elasticsearch response.
     */
    default void ok(JsonObject item) {
    }

    /**
     * @return status object; typically called after the bulk indexer has been closed.
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

    default void flush() {
    }
}
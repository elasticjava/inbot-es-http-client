package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;

import com.github.jsonj.JsonObject;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

public class LoggingStatusHandler implements BulkIndexerStatusHandler {
    private final Logger log;
    private final boolean verbose;
    public LoggingStatusHandler(Logger log) {
        this.log = log;
        this.verbose = false;
    }

    public LoggingStatusHandler(Logger log, boolean verbose) {
        this.log = log;
        this.verbose = verbose;
    }

    AtomicLong ok = new AtomicLong();
    AtomicLong error = new AtomicLong();

    @Override
    public JsonObject status() {
        return object(
                field("reindex_success", error.get()==0),
                field("total", ok.get() + error.get()),
                field("indexed", ok.get()),
                field("errors", error.get()
            ));
    }

    @Override
    public void ok(JsonObject item) {
        ok.incrementAndGet();
    }

    @Override
    public void fail(String message) {
        log.error("fail " + message);
    }

    @Override
    public void error(String code, JsonObject details) {
        error.incrementAndGet();
        log.error("error" + code + " " + details.toString());
    }

    @Override
    public void start() {
        log.info("bulk index");
    }

    @Override
    public void done() {
        log.info(status().toString());
    }

    @Override
    public void flush() {
        if(verbose) {
            log.info(status().toString());
        }
    }
}
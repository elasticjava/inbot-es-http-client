package io.inbot.elasticsearch.exceptions;

import com.github.jsonj.JsonObject;

public class EsUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 5119255345999656528L;

    public EsUnavailableException(String problem) {
        super(problem);
    }

    public EsUnavailableException(JsonObject details) {
        super(details.toString());
    }
}
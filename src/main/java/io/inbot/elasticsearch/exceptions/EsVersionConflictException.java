package io.inbot.elasticsearch.exceptions;

import com.github.jsonj.JsonObject;

public class EsVersionConflictException extends RuntimeException {
    private static final long serialVersionUID = -3357729391843579340L;

    public EsVersionConflictException() {
        super("Version conflict");
    }

    public EsVersionConflictException(JsonObject summary) {
        super("Version conflict: " + summary);
    }
}
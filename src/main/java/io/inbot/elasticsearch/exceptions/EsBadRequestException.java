package io.inbot.elasticsearch.exceptions;

import com.github.jsonj.JsonObject;

public class EsBadRequestException extends RuntimeException {

    private static final long serialVersionUID = 7106769721007412846L;

    public EsBadRequestException(String problem) {
        super(problem);
    }

    public EsBadRequestException(JsonObject details) {
        super(details.toString());
    }
}
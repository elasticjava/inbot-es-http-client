package io.inbot.elasticsearch.exceptions;

public class EsConnectionException extends IllegalStateException {

    private static final long serialVersionUID = -3251262428494960004L;

    public EsConnectionException(String message, Throwable t) {
        super(message, t);
    }
}

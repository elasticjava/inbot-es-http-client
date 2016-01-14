package io.inbot.elasticsearch.exceptions;

public class EsNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 503732628676593718L;

    public EsNotFoundException() {
        super("not found");
    }

    public EsNotFoundException(String message) {
        super(message);
    }
}
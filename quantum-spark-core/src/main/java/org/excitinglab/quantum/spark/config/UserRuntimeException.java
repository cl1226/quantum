package org.excitinglab.quantum.spark.config;

/**
 */
public class UserRuntimeException extends RuntimeException {

    public UserRuntimeException() {
        super();
    }

    public UserRuntimeException(String message) {
        super(message);
    }

    public UserRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UserRuntimeException(Throwable cause) {
        super(cause);
    }
}

package com.example.exception;

/**
 * Wrapper around IOException possible while accessing files
 */
public class FileQueueException extends RuntimeException {

    public FileQueueException(String message) {
        super(message);
    }

    public FileQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}

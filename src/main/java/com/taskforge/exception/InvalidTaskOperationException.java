package com.taskforge.exception;

public class InvalidTaskOperationException extends RuntimeException {
    
    public InvalidTaskOperationException(String message) {
        super(message);
    }
}

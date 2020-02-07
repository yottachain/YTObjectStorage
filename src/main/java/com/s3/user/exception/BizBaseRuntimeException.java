package com.s3.user.exception;

/**
 * 自定义运行时异常基类
 */
public abstract class BizBaseRuntimeException extends RuntimeException implements BizBaseException{
    private static final long serialVersionUID = 1L;

    public BizBaseRuntimeException() {

    }

    public BizBaseRuntimeException(String message) {
        super(message);
    }

    public BizBaseRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}

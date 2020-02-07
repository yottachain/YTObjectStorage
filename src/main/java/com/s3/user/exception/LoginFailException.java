package com.s3.user.exception;


/**
 * 登录失败异常基类
 */
public abstract class LoginFailException extends CommonBizException{

    public LoginFailException() {
    }

    public LoginFailException(String message) {
        super(message);
    }

    public LoginFailException(String message, Throwable cause) {
        super(message, cause);
    }
}

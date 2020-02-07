package com.s3.user.exception;


import de.mindconsulting.s3storeboot.constants.ErrInfo;

/**
 * 通用业务异常基类
 */
public abstract class CommonBizException extends BizBaseRuntimeException {
    /**
     * 展示给前端看的错误消息参数
     */
    protected String[] restErrMsgArgs;

    /**
     * 错误详情对象, 包含各类错误对象
     */
    protected ErrInfo errInfo;

    public CommonBizException() {
        super();
    }

    public CommonBizException(String message) {
        super(message);
    }

    public CommonBizException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommonBizException setRestErrMsgArgs(String[] restErrMsgArgs) {
        this.restErrMsgArgs = restErrMsgArgs;
        return this;
    }

    public CommonBizException setErrInfo(ErrInfo errInfo) {
        this.errInfo = errInfo;
        return this;
    }

    public String[] getRestErrMsgArgs() {
        return restErrMsgArgs;
    }

    public ErrInfo getErrInfo() {
        return errInfo;
    }
}

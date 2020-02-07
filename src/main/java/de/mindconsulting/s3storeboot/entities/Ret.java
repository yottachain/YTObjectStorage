package de.mindconsulting.s3storeboot.entities;

import com.google.common.base.MoreObjects;
import de.mindconsulting.s3storeboot.constants.ErrCode;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.core.annotation.Order;

import java.io.Serializable;

/**
 * restapi的返回值json结果
 *
 * @param <T>
 */
@ApiModel(value = "请求响应")
public class Ret<T> implements Serializable {
    @Order(1)
    @ApiModelProperty(value = "错误码")
    private String code;

    @Order(2)
    @ApiModelProperty(value = "错误描述")
    private String msg;

    @Order(3)
    @ApiModelProperty(value = "请求时间")
    private Long ctime;

    @Order(4)
    @ApiModelProperty(value = "请求编号")
    private String requestID;

    @Order(5)
    @ApiModelProperty(value = "响应数据")
    private T data;

    public Ret() {
        this.ctime = System.currentTimeMillis();
    }

    public Ret(String code) {
        this.code = code;
        this.ctime = System.currentTimeMillis();
    }

    public Ret(String code, String msg) {
        this.code = code;
        this.msg = msg;
        this.ctime = System.currentTimeMillis();
    }

    //构建成功返回
    public static Ret ok() {
        return new Ret(ErrCode.M0000.getCode(), "ok");
    }

    //构建系统异常返回
    public static Ret error() {
        return new Ret(ErrCode.M1000.getCode(),"error");
    }


    public String getCode() {
        return code;
    }

    public Ret<T> setCode(String code) {
        this.code = code;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public Ret<T> setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public Long getCtime() {
        return ctime;
    }

    public Ret<T> setCtime(Long ctime) {
        this.ctime = ctime;
        return this;
    }

    public String getRequestID() {
        return requestID;
    }

    public Ret<T> setRequestID(String requestID) {
        this.requestID = requestID;
        return this;
    }

    public T getData() {
        return data;
    }

    public Ret<T> setData(T data) {
        this.data = data;
        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("\ncode", code)
                .add("\nmsg", msg)
                .add("\nctime", ctime)
                .add("\ndata", data)
                .toString();
    }
}


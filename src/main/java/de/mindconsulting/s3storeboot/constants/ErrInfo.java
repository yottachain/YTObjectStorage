package de.mindconsulting.s3storeboot.constants;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ErrInfo {
    /**
     * 发生错误时的对象
     * 可以是一个String 如: id, 也可以是一个对象, 即一组参数
     */
    protected Object errObj;

    /**
     * 造成错误的对象
     * 如: 文件名已存在, 那么需要告诉用户是哪个文件id造成的重名
     */
    protected Object causeErrObj;

    public ErrInfo(Object errObj, Object causeErrObj) {
        this.errObj = errObj;
        this.causeErrObj = causeErrObj;
    }

    public ErrInfo setErrObj(Object errObj) {
        this.errObj = errObj;
        return this;
    }

    public ErrInfo setCauseErrObj(Object causeErrObj) {
        this.causeErrObj = causeErrObj;
        return this;
    }
}

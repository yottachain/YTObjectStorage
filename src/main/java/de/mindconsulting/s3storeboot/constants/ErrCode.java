package de.mindconsulting.s3storeboot.constants;


import com.s3.user.exception.BizBaseException;

/**
 * 所有错误消息编码
 * 某一个异常可以对应一个错误编码
 */
public enum ErrCode {
    ///////////////////////////////////////////
    ///////////  登录错误   ////////////////////
    ///////////////////////////////////////////
    M0000("200"),//成功
    M1000("M1000", com.s3.user.exception.LoginFailException.class),
    M5001("M5001"); //service级别的Validation错误

    ////////////////////////////////////////////////////////////////
    private String code; //错误编码

    private Class<? extends BizBaseException> ex;

    ErrCode(String code) {
        this.code = code;
    }

    ErrCode(String code, Class<? extends BizBaseException> ex) {
        this.code = code;
        this.ex = ex;
    }

    /**
     * 根据异常来获取当前ErrCode
     *
     * @param exception
     * @return
     */
    public static ErrCode get(BizBaseException exception) {
        Class<? extends BizBaseException> clazz = exception.getClass();

        ErrCode ret = null;

        for (ErrCode ec : values()) {
            if (ec.ex == clazz) {
                ret = ec;
                break;
            }
        }

        //如果未找到相应的ErrCode, 则用 M5001 代替
        if(ret == null){
            ret = ErrCode.M5001;;
        }
        return ret;
    }
    public String getCode() {
        return code;
    }
}

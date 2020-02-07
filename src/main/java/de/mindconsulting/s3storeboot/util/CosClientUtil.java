package de.mindconsulting.s3storeboot.util;
import java.io.File;

public class CosClientUtil {

    public static String get_cos_key(String dirctory,String key) throws Exception{
        String cos_key="5dc3383e69bd32ca72696e98d31d4c65a4b06c60f962bdf7239d50f2e58ec6c8";
        String license = dirctory +"/"+ "license.lic";
        File file = new File(license);
        byte[] data = AESUtil.getBytesFromFile(file);
        String aesString = new String(data);
        String code = AESCoder.decrypt(cos_key,aesString);
        return code;
    }

}

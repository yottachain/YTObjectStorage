package de.mindconsulting.s3storeboot.util;

import org.apache.commons.lang3.StringUtils;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AESCoder{
    private static final String ivStr = "43720ui239062387";
    public static String encrypt(String strKey, String strIn) throws Exception {
        if(StringUtils.isEmpty(strKey)) {
            throw new Exception("decrypt string can't null or empty");
        }
        if(StringUtils.isEmpty(strIn)) {
            throw new Exception("decrypt string can't null or empty");
        }

        SecretKeySpec skeySpec = getKey(strKey);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = new IvParameterSpec(ivStr.getBytes());
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
        byte[] encrypted = cipher.doFinal(strIn.getBytes("utf-8"));
        BASE64Encoder encoder = new BASE64Encoder();
        return encoder.encode(encrypted);
    }

    public static String decrypt(String strKey, String strIn) throws Exception {
        if(StringUtils.isEmpty(strKey)) {
            throw new Exception("decrypt string can't null or empty");
        }
        if(StringUtils.isEmpty(strIn)) {
            throw new Exception("decrypt string can't null or empty");
        }

        SecretKeySpec skeySpec = getKey(strKey);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = new IvParameterSpec(ivStr.getBytes());
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

        BASE64Decoder decoder = new BASE64Decoder();
        byte[] encrypted1 = decoder.decodeBuffer(strIn);
        byte[] original = cipher.doFinal(encrypted1);
        String originalString = new String(original, "utf-8");
        return originalString;
    }

    private static SecretKeySpec getKey(String strKey) throws Exception {
        byte[] arrBTmp = strKey.getBytes();
        byte[] arrB = new byte[16]; // 创建一个空的16位字节数组（默认值为0）
        for (int i = 0; i < arrBTmp.length && i < arrB.length; i++) {
            arrB[i] = arrBTmp[i];
        }
        SecretKeySpec skeySpec = new SecretKeySpec(arrB, "AES");

        return skeySpec;
    }

    public static void main(String[] args) throws Exception {
        String Code = "中文ABc123";
        String key = "qwertyuiop@12345";
        String codE;
        codE = AESCoder.encrypt(key, Code);
        System.out.println("原文：" + Code);
        System.out.println("密钥：" + key);
        System.out.println("密文：" + codE);
        System.out.println("解密：" + AESCoder.decrypt(key, codE));
    }
}
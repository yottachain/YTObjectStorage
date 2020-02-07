package de.mindconsulting.s3storeboot.util;

import com.ytfs.common.codec.AESCoder;

import javax.crypto.Cipher;
import java.io.*;

public class AESUtil {

    public static byte[] getBytesFromFile(File file) throws Exception {
        InputStream is = new FileInputStream(file);
        // 获取文件大小
        long length = file.length();
        if (length > Integer.MAX_VALUE) {
            // 文件太大，无法读取
            throw new IOException("File is to large "+file.getName());
        }
        // 创建一个数据来保存文件数据
        byte[] bytes = new byte[(int)length];
        // 读取数据到byte数组中
        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
            offset += numRead;
        }
        // 确保所有数据均被读取
        if (offset < bytes.length) {
            throw new IOException("Could not completely read file "+file.getName());
        }
        // Close the input stream and return bytes
        is.close();
        return bytes;
    }


    /**
     * 根据byte数组，生成解密文件
     */
    public static void getDecryptFile(byte[] bfile, String filePath,String fileName) {
        BufferedOutputStream bos = null;  //新建一个输出流
        FileOutputStream fos = null;  //w文件包装输出流
        File file = null;
        try {
            File dir = new File(filePath);
            if (!dir.exists() && dir.isDirectory()) {//判断文件目录是否存在
                dir.mkdirs();
            }
            file = new File(filePath + "/" + fileName);  //新建一个file类
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);  //输出的byte文件

            AESCoder coder = new AESCoder(Cipher.DECRYPT_MODE);
            byte[] data = coder.doFinal(bfile);
            bos.write(data);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();  //关闭资源
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();  //关闭资源
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

}

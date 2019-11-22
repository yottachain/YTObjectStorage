package de.mindconsulting.s3storeboot.service;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.region.Region;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.conf.UserConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CosBackupService {

    public static long appId = 1258989317l;
    public static String secretId = "AKIDBUxkRsyFAwfLYZ7EpfQ6c3NbxDIK7ths";
    public static String secretKey = "hlnlYAbrwqZ7UhmqpeLm29mGfz8SbbVE";
    static String bucketName = "tmpupload-yotta"+"-"+appId;
    static COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);
    static ClientConfig clientConfig = new ClientConfig(new Region("ap-beijing"));
    static COSClient cosClient = new COSClient(cred, clientConfig);


    public static boolean checkIsExist(String fileName){

        return false;
    }

    public static String uploadFile(String localFilePath,String bucket,String objectKey) {
        File localFile = new File(localFilePath);
        String fileName = UserConfig.userId+"/"+bucket+"/"+objectKey;
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,fileName,localFile);
        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
        String etag = putObjectResult.getETag();
        cosClient.shutdown();
        return etag;
    }


    public static long copy(InputStream in, OutputStream out,OutputStream aes) throws IOException, InterruptedException {
        long byteCount = 0;
        byte[] buffer = new byte[4096];
        int bytesRead = -1;
        while ((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);
            byte[] AESKey = UserConfig.AESKey;
            buffer = KeyStoreCoder.aesEncryped(buffer,AESKey);
            aes.write(buffer,0,buffer.length);
            byteCount += bytesRead;
            Thread.sleep(0);
        }

        out.flush();
        aes.flush();
        return byteCount;
    }

}

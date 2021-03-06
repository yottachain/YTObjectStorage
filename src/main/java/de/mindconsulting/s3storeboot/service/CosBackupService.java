package de.mindconsulting.s3storeboot.service;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.region.Region;
import com.ytfs.common.codec.AESCoder;
import com.ytfs.common.conf.UserConfig;
import de.mindconsulting.s3storeboot.util.CosClientUtil;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Configuration;

import javax.crypto.Cipher;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;


@Configuration
public class CosBackupService {

//    @Value("${s3server.dirctory}")
//    String dirctory;

//    @Value("${s3server.cos_key}")
//    String cos_key;



    private static final Logger LOG = Logger.getLogger(CosBackupService.class);
    public static long appId = 1258989317l;
    public static String bucketName = "tmpupload-yotta"+"-"+appId;
    public static String cos_key = "5dc3383e69bd32ca72696e98d31d4c65a4b06c60f962bdf7239d50f2e58ec6c8";
    public static String dirctory = "../conf";

    public COSClient getClient() {
        LOG.info("directory====="+dirctory + " ,cos_key======"+cos_key);
        String jsonStr = null;
        try {
            jsonStr = CosClientUtil.get_cos_key(dirctory,cos_key);
        } catch (Exception e) {
            LOG.error("ERR:::",e);
            e.printStackTrace();
        }

        if(!"".equals(jsonStr)) {
            JSONObject json = JSONObject.fromObject(jsonStr);
            COSCredentials cred = new BasicCOSCredentials(json.getString("secretId"), json.getString("secretKey"));
            ClientConfig clientConfig = new ClientConfig(new Region("ap-beijing"));
            COSClient cosClient = new COSClient(cred, clientConfig);

            return cosClient;
        }   else {
            return null;
        }

    }

    public static boolean checkIsExist(String fileName){

        return false;
    }

    public String uploadFile(String localFilePath,String bucket,String objectKey,String cos_bucket) {
        COSClient cosClient = this.getClient();
        String bucketName = cos_bucket + "-"+appId;
        File localFile = new File(localFilePath);
        String fileName = UserConfig.userId+"_"+bucket+"_"+objectKey;
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,fileName,localFile);
        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
        String etag = putObjectResult.getETag();
        cosClient.shutdown();
        return etag;
    }


    public static long copy(InputStream in, OutputStream out,OutputStream aes,String cosBackUp) throws Exception {
        long byteCount = 0;
        byte[] buffer = new byte[4096];
        int bytesRead = -1;
        //腾讯云备份*************
        AESCoder coder = new AESCoder(Cipher.ENCRYPT_MODE);
        //腾讯云备份*************
        while ((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);

            //腾讯云备份*************
            if("on".equals(cosBackUp)) {
                byte[] data = coder.update(buffer,0,bytesRead);
                aes.write(data);
            }
            //腾讯云备份*************
            byteCount += bytesRead;
        }


        //腾讯云备份*************
        if("on".equals(cosBackUp)) {
            byte[] data2 = coder.doFinal();
            aes.write(data2);
            aes.flush();
        }

        //腾讯云备份*************

        out.flush();

        return byteCount;
    }

}

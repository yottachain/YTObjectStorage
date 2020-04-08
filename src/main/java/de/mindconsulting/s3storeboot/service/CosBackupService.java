package de.mindconsulting.s3storeboot.service;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferManagerConfiguration;
import com.qcloud.cos.transfer.Upload;
import com.s3.user.controller.sync.task.AyncFileMeta;
import com.ytfs.client.v2.YTClient;
import com.ytfs.client.v2.YTClientMgr;
import com.ytfs.common.codec.AESCoder;
import com.ytfs.common.conf.UserConfig;
import de.mc.ladon.s3server.common.PropertiesUtil;
import de.mc.ladon.s3server.common.S3Constants;
import de.mc.ladon.s3server.entities.api.S3CallContext;
import de.mc.ladon.s3server.entities.api.S3ResponseHeader;
import de.mc.ladon.s3server.entities.impl.S3ResponseHeaderImpl;
import de.mc.ladon.s3server.jaxb.entities.Part;
import de.mindconsulting.s3storeboot.util.AESDecryptInputStream;
import de.mindconsulting.s3storeboot.util.CosClientUtil;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Configuration;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Configuration
public class CosBackupService {

    private static final Logger LOG = Logger.getLogger(CosBackupService.class);
        public static long appId = 1258989317l;
//    public static String bucketName = "yotta"+userId%180+"-"+appId;
    public static String cos_key = "5dc3383e69bd32ca72696e98d31d4c65a4b06c60f962bdf7239d50f2e58ec6c8";
    public static String dirctory = "../conf";
    public static String regionid = "ap-beijing";

    public static COSClient getClient() {
        String jsonStr = null;
        try {
            jsonStr = CosClientUtil.get_cos_key(dirctory,cos_key);
        } catch (Exception e) {
            LOG.error("ERR:::",e);
            e.printStackTrace();
        }
        JSONObject json = JSONObject.fromObject(jsonStr);
        COSCredentials cred = new BasicCOSCredentials(json.getString("appId"),json.getString("secretId"), json.getString("secretKey"));
        ClientConfig clientConfig = new ClientConfig(new Region(regionid));
        COSClient cosclient = new COSClient(cred, clientConfig);
        return cosclient;
    }

//    public static COSClient getClient() {
//        LOG.info("directory====="+dirctory + " ,cos_key======"+cos_key);
//        String jsonStr = null;
//        try {
//            jsonStr = CosClientUtil.get_cos_key(dirctory,cos_key);
//        } catch (Exception e) {
//            LOG.error("ERR:::",e);
//            e.printStackTrace();
//        }
//
//        if(!"".equals(jsonStr)) {
//            JSONObject json = JSONObject.fromObject(jsonStr);
//            COSCredentials cred = new BasicCOSCredentials(json.getString("secretId"), json.getString("secretKey"));
//            ClientConfig clientConfig = new ClientConfig(new Region("ap-beijing"));
//            COSClient cosClient = new COSClient(cred, clientConfig);
//
//            return cosClient;
//        }   else {
//            return null;
//        }
//
//    }



    public String uploadFile(AyncFileMeta req) {
        String jsonStr = null;
        String appId = null;
        try {
            jsonStr = CosClientUtil.get_cos_key(dirctory,cos_key);
        } catch (Exception e) {
            LOG.error("ERR:::",e);
            e.printStackTrace();
        }
        if(!"".equals(jsonStr)) {
            JSONObject json = JSONObject.fromObject(jsonStr);
            appId = json.getString("appId");
        }
        PropertiesUtil p = new PropertiesUtil("../bin/application.properties");
        String securityEnabled = p.readProperty("s3server.securityEnabled");

//        LOG.info("appId = "+appId);
        int userId = 0 ;
        if(securityEnabled.equals("true")){
            String publicKey = req.getPublicKey();
//            String new_publicKey = publicKey.substring(publicKey.indexOf("YTA")+3);
            YTClient client = YTClientMgr.getClient(publicKey);
            userId = client.getUserId();
        }else {
            userId = UserConfig.userId;
        }

        int mod = userId%180;
        LOG.info("USER_ID = " + userId+",mod = "+mod);
        String bucketName = "yotta"+mod+"-"+appId;
        File localFile = new File(req.getAesPath());
        String fileName = userId+"_"+req.getBucketname()+"_"+req.getKey();
        String etag = null;
        long fileSize = localFile.length();
        long maxFileSize = 5368709120L;
        if(fileSize>maxFileSize) {
            LOG.info("BIG FILE UPLOAD.....");
            etag = uploadBigFile(req.getAesPath(),bucketName,fileName);
            LOG.info("BIG FILE UPLOAD OVER...");
        }else{
            try{
                PutObjectResult putObjectResult = this.upload(bucketName,fileName,localFile);
                LOG.info("COS SUCCESS,COS OVER ,"+fileName);
                etag = putObjectResult.getETag();
            }catch (Exception e){
                LOG.error("cos upload failed");
            }
        }
        return etag;
    }

    public PutObjectResult upload(String bucket,String fileName,File localFile){
        COSClient cosClient = this.getClient();
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket,fileName,localFile);
        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
        cosClient.shutdown();
        return putObjectResult;
    }


    public String uploadBigFile(String localFilePath,String bucket,String objectKey){
        COSClient cosClient = getClient();
        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        TransferManager transferManager = new TransferManager(cosClient, threadPool);
// 设置高级接口的分块上传阈值和分块大小为10MB
        TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
        transferManagerConfiguration.setMultipartUploadThreshold(10 * 1024 * 1024);
        transferManagerConfiguration.setMinimumUploadPartSize(10 * 1024 * 1024);
        transferManager.setConfiguration(transferManagerConfiguration);
        File file = new File(localFilePath);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, objectKey, file);
        // 本地文件上传
        Upload upload = transferManager.upload(putObjectRequest);
        // 等待传输结束（如果想同步的等待上传结束，则调用 waitForCompletion）
        UploadResult uploadResult = null;
        try {
            uploadResult = upload.waitForUploadResult();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String Etag = uploadResult.getETag();
        transferManager.shutdownNow();
        cosClient.shutdown();
        return Etag;
    }

    public void downloadFile(S3CallContext callContext,String bucket, String fileName,long fileSize,boolean head) {
        COSClient cosClient = this.getClient();

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket,fileName);

        COSObject cosObject = cosClient.getObject(getObjectRequest);
        String ETag = cosObject.getObjectMetadata().getETag();
        String contentType = cosObject.getObjectMetadata().getContentType();
        COSObjectInputStream cosObjectInputStream = cosObject.getObjectContent();
        S3ResponseHeader header = new S3ResponseHeaderImpl();
        header.setContentLength(fileSize);
        header.setEtag(ETag);
        header.setContentType(contentType);
        header.setConnection(S3Constants.Connection.valueOf("open"));
        callContext.setResponseHeader(header);
        try {
            if(!head){
                String securityEnabled = this.getValue("s3server.securityEnabled");
                if(securityEnabled.equals("true")) {
                    String publicKey  = callContext.getUser().getPublicKey();
                    String new_publiceKey = publicKey.substring(publicKey.indexOf("YTA")+3);
                    YTClient client = YTClientMgr.getClient(new_publiceKey);
                    callContext.setContent(new AESDecryptInputStream(cosObjectInputStream,client.getAESKey()));
                }else {
                    callContext.setContent(new AESDecryptInputStream(cosObjectInputStream,UserConfig.AESKey));
                }

            }
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }
        LOG.info("Download [" + fileName + "] is Success..." );
    }

    public String getValue(String parmars){
        String filePath = "../bin/application.properties";
        PropertiesUtil p = new PropertiesUtil(filePath);
        String ss = p.readProperty(parmars);
        return ss;
    }

    public byte[] getBytesFromFile(COSObjectInputStream inputStream,long file_size) throws Exception {

        // 创建一个数据来保存文件数据
        byte[] bytes = new byte[(int) file_size];
        // 读取数据到byte数组中
        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length && (numRead=inputStream.read(bytes, offset, bytes.length-offset)) >= 0) {
            offset += numRead;
        }
        // 确保所有数据均被读取
        if (offset < bytes.length) {
            throw new IOException("Could not completely read file ");
        }
        // Close the input stream and return bytes
        inputStream.close();
        return bytes;

    }



    public List<COSObjectSummary> listObjects(ListObjectsRequest request) throws CosClientException{
        COSClient cosClient = this.getClient();
        String bucketName = "upload-yotta-1258989317";
//        String prefix = "39_";
        request.setBucketName(bucketName);
//        request.setPrefix(prefix);
        request.setMaxKeys(1000);
        ObjectListing objectListing = null;
        List<COSObjectSummary> cosObjectSummaries1 = new ArrayList<>();
        do {
            try {
                objectListing = cosClient.listObjects(request);
            } catch (CosServiceException e) {
                e.printStackTrace();
            } catch (CosClientException e) {
                e.printStackTrace();
            }
            // common prefix表示表示被delimiter截断的路径, 如delimter设置为/, common prefix则表示所有子目录的路径
            List<String> commonPrefixs = objectListing.getCommonPrefixes();

            // object summary表示所有列出的object列表
            // object summary表示所有列出的object列表
            List<COSObjectSummary> cosObjectSummaries = objectListing.getObjectSummaries();
            for (COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                LOG.info("key:" + cosObjectSummary.getKey());
                LOG.info("ETag:"+cosObjectSummary.getETag());
                cosObjectSummaries1.add(cosObjectSummary);
            }

            String nextMarker = objectListing.getNextMarker();
            request.setMarker(nextMarker);
        } while (objectListing.isTruncated());
        cosClient.shutdown();
        return cosObjectSummaries1;
    }
    public void copyObject(String prefix){
        ListObjectsRequest request = new ListObjectsRequest();
        request.setPrefix(prefix);
        List<COSObjectSummary> cosObjectSummaries = this.listObjects(request);
//        String prefix = "39_";
        String pre = prefix.substring(0,prefix.indexOf("_"));
        int userId = Integer.parseInt(pre);
        int mod = userId%180;
        LOG.info("mod ===" + mod);
        String sourceBucketName = "upload-yotta-1258989317";
        String destBucketName = "yotta"+mod+"-"+"1258989317";

        if(cosObjectSummaries.size()>0) {
            LOG.info("用户:" + userId + " 有" + cosObjectSummaries.size() + " 个文件");
            int count = 0;
            COSClient cosClient = this.getClient();
            for(COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                CopyObjectRequest copyObjectRequest = new CopyObjectRequest(sourceBucketName,cosObjectSummary.getKey(),destBucketName,cosObjectSummary.getKey());

                LOG.info("开始迁移文件：【 "+cosObjectSummary.getKey() + " 】 从 存储桶:"+sourceBucketName +" 至存储桶 "+ destBucketName);
                cosClient.copyObject(copyObjectRequest);
                count ++;
                int a = cosObjectSummaries.size() - count ;
                LOG.info("用户：" +userId + "文件：" + cosObjectSummary.getKey() +" 迁移完成,当前是第" + count + " 个文件,还剩" + a + "个文件");
                LOG.info("删除原存储桶中数据");
                cosClient.deleteObject(sourceBucketName,cosObjectSummary.getKey());

            }
            LOG.info("用户: " + userId  + " 成功迁移 " + count  + " 个文件");
            cosClient.shutdown();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static long copy(S3CallContext callContext,InputStream in, OutputStream out,OutputStream aes,String cosBackUp) throws Exception {
        long byteCount = 0;
        byte[] buffer = new byte[131072];
        int bytesRead = -1;
        //腾讯云备份*************
        PropertiesUtil p = new PropertiesUtil("../bin/application.properties");
        String securityEnabled = p.readProperty("s3server.securityEnabled");
        AESCoder coder=null;
        if(securityEnabled.equals("true")) {
            String publicKey = callContext.getUser().getPublicKey();
            String new_publicKey = publicKey.substring(publicKey.indexOf("YTA")+3);
            YTClient client = YTClientMgr.getClient(new_publicKey);
            coder = new AESCoder(Cipher.ENCRYPT_MODE,client.getAESKey());
        }else {
            coder = new AESCoder(Cipher.ENCRYPT_MODE);
        }
        //腾讯云备份*************
        while ((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);

            //腾讯云备份*************
            if(!"false".equals(cosBackUp)) {
                byte[] data = coder.update(buffer,0,bytesRead);
                aes.write(data);
            }
            //腾讯云备份*************
            byteCount += bytesRead;
        }

        //腾讯云备份*************
        if(!"false".equals(cosBackUp)) {
            byte[] data2 = coder.doFinal();
            aes.write(data2);
            aes.close();
            aes.flush();
        }

        //腾讯云备份*************
        out.close();
        out.flush();

        return byteCount;
    }

    public List<Part> sortList(List<Part> parts) {
        Collections.sort(parts, new Comparator<Part>() {
            @Override
            public int compare(Part o1, Part o2) {
                if(o1.getPartNumber()<o2.getPartNumber()){
                    return -1;
                } else {
                    return 1;
                }
            }
            @Override
            public boolean equals(Object obj) {
                return false;
            }
        });
        return parts;
    }

    public static void main(String []args) {
        CosBackupService service = new CosBackupService();
        for(int i=0;i<=555;i++) {
            String prefix = i + "_";
            LOG.info("开始迁移用户: " + i + " 的数据") ;
            try{
                service.copyObject(prefix);
            }catch (Exception e) {
                LOG.info("ERR:",e);
            }


        }
    }

}

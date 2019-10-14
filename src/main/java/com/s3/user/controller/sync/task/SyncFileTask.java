package com.s3.user.controller.sync.task;

import com.ytfs.service.packet.s3.UploadFileReq;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Component
@Order(4)
public class SyncFileTask implements ApplicationRunner {
    private static final Logger LOG = Logger.getLogger(SyncFileTask.class);

    @Value("${s3server.SYNC_DIR}")
    String SYNC_DIR;
    @Value("${s3server.SYNC_BUCKET}")
    String syncBucketName;

    private String jobName = "syncFileJob";


    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info("【系统启动】初始化异步文件上传任务...");
        LOG.info("SYNC_DIR===="+SYNC_DIR);
        Path syncDir = Paths.get(SYNC_DIR+"/"+syncBucketName);
        if (!Files.exists(syncDir)) {
            Files.createDirectories(syncDir);
        }
        String[] objectList = new File(SYNC_DIR+"/"+syncBucketName).list();


        LOG.info("File count::::"+objectList.length);
        if(objectList.length > 0) {
            for(int i=0;i<objectList.length;i++) {
                String filePath =syncDir.toString() +"/"+ objectList[i];
                UploadFileReq req = new UploadFileReq();
                req.setFilePath(filePath);
                LOG.info("File path is ::::"+filePath);

                SyncUploadSenderPool.startSender(i,req);
//
//                File file = new File(filePath);
//
//                Map<String, String> header = new HashMap<>();
//                header.put("contentLength",file.length()+"");
//                header.put("x-amz-date",(new Date()).getTime()+"");
//                byte[] bs = SerializationUtil.serializeMap(header);
//
//                UploadObject uploadObject = new UploadObject(file.getPath());
//                uploadObject.upload();
//                ObjectHandler.createObject(syncBucketName, file.getName(), uploadObject.getVNU(), bs);
//
//                LOG.info(objectList[i]+" uploaded successfully................");
//                //删除缓存文件
//                LOG.info("Delete ******* CACHE FILE...........");
//                Path obj = Paths.get(filePath);
//                Files.delete(obj);
            }
        }
//        LOG.info("【系统启动】初始化异步文件上传任务over");
    }


//    private byte[] writeMetaFile(String filePath) {
//        File file = new File(filePath);
//        long contentLength = file.length();
//        Map<String,String> header = new HashMap<>();
//
//    }

}

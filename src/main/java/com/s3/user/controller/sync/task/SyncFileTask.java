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
import java.util.concurrent.ArrayBlockingQueue;

@Configuration
@Component
@Order(4)
public class SyncFileTask implements ApplicationRunner {
    private static final Logger LOG = Logger.getLogger(SyncFileTask.class);

    @Value("${s3server.SYNC_DIR}")
    String SYNC_DIR;
    @Value("${s3server.SYNC_BUCKET}")
    String syncBucketName;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info("【系统启动】初始化异步文件上传任务...");
        LOG.info("SYNC_DIR===="+SYNC_DIR);
        int count = SyncUploadSenderPool.newInstance().syncCount;
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

                //在队列长度允许的情况下，可以往队列加  队列长度可配置 这里的50就表示当前队列的长度
                if(i < 50) {
                    SyncUploadSenderPool.startSender(req);
                }
            }
        }
    }



}

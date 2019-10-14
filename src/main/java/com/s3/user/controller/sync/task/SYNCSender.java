package com.s3.user.controller.sync.task;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.s3.UploadFileReq;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SYNCSender extends Thread{

    private static final Logger LOG = Logger.getLogger(SYNCSender.class);

    private final ArrayBlockingQueue<UploadFileReq> queue;

    private final int count;
//    private static SYNCSender[] senders;

    public SYNCSender(int ss,ArrayBlockingQueue<UploadFileReq> queue) {

        this.queue = queue;
        this.count = ss;
    }

    public static SYNCSender startSender(int ii,ArrayBlockingQueue<UploadFileReq> queue) {

        SYNCSender sender = new SYNCSender(ii,queue);

        sender.start();
        return sender;
    }

    public void stopSend() {
        this.interrupt();

        try {
            this.join(15000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }

    public void putMessage(UploadFileReq req) {
        try {
            queue.put(req);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        UploadFileReq req;
        String bucketName = SyncUploadSenderPool.newInstance().syncBucketName;

        while (!this.isInterrupted()) {
            try {
                req = queue.take();
            }catch (InterruptedException e) {
                break;
            }
            LOG.info("path"+req.getFilePath());
            String filePath = req.getFilePath();
            File file = new File(filePath);
            Map<String, String> header = new HashMap<>();
            header.put("contentLength",file.length()+"");
            header.put("x-amz-date",(new Date()).getTime()+"");
            byte[] bs = SerializationUtil.serializeMap(header);

            UploadObject uploadObject = null;
            try {
                uploadObject = new UploadObject(file.getPath());
                uploadObject.upload();
                ObjectHandler.createObject(bucketName, file.getName(), uploadObject.getVNU(), bs);
            } catch (IOException | InterruptedException | ServiceException e) {
                e.printStackTrace();
            }
            LOG.info(file.getName() +" uploaded successfully................");
            //删除缓存文件
            LOG.info("Delete ******* CACHE FILE...........");
            Path obj = Paths.get(filePath);
            try {
                Files.delete(obj);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}

package com.s3.user.controller.sync.task;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.ObjectHandler;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ArrayBlockingQueue;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SYNCSender extends Thread {

    private static final Logger LOG = Logger.getLogger(SYNCSender.class);
    private final ArrayBlockingQueue<AyncFileMeta> queue;

    private final int threadId;

    public SYNCSender(int ss, ArrayBlockingQueue<AyncFileMeta> queue) {
        this.queue = queue;
        this.threadId = ss;
    }

    public static SYNCSender startSender(int ii, ArrayBlockingQueue<AyncFileMeta> queue) {
        SYNCSender sender = new SYNCSender(ii, queue);
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


    @Override
    public void run() {
        AyncFileMeta req = null;
        while (!this.isInterrupted()) {
            try {
                if (req == null) {
                    req = queue.take();
                }
            } catch (InterruptedException e) {
                break;
            }
            LOG.info("path" + req.getPath());
            String filePath = req.getPath();
            File file = new File(filePath);
            byte[] bs = req.getMeta();
            UploadObject uploadObject = null;
            try {
               
                //file.getPath()//不存在，认为成功
                        
                        
                uploadObject = new UploadObject(file.getPath());
                uploadObject.upload();
                ObjectHandler.createObject(req.getBucketname(), req.getKey(), uploadObject.getVNU(), bs);
                req = null;
            } catch (Throwable e) {
                LOG.error("", e);
            }
            LOG.info(file.getName() + " uploaded successfully................");
            //删除缓存文件
            LOG.info("Delete ******* CACHE FILE...........");
            Path obj = Paths.get(filePath);
            try {
                Files.delete(obj);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //当线程队列等待长度为0，但是缓存目录下依然有文件需要 将文件继续put进队列
//        String SYNC_DIR = SyncUploadSenderPool.newInstance().SYNC_DIR;
//        String syncBucketName = SyncUploadSenderPool.newInstance().syncBucketName;
//        String[] objectList = new File(SYNC_DIR+"/"+syncBucketName).list();
//        Path syncDir = Paths.get(SYNC_DIR+"/"+syncBucketName);
//        if(queue.remainingCapacity() == queue.size() && objectList.length>0) {
//            for(int i=0;i<objectList.length;i++) {
//                String filePath =syncDir.toString() +"/"+ objectList[i];
//                UploadFileReq req1 = new UploadFileReq();
//                req1.setFilePath(filePath);
//                //在队列长度允许的情况下，可以往队列加  队列长度可配置 这里的50就表示当前队列的长度
//                if(i < 50) {
//                    SyncUploadSenderPool.startSender(req1);
//                }
//            }
//        }
    }
}

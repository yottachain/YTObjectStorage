package com.s3.user.controller.sync.task;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class AyncSender extends Thread {

    private static final Logger LOG = Logger.getLogger(AyncSender.class);
    private final ArrayBlockingQueue<AyncFileMeta> queue;

    private final int threadId;

    public AyncSender(int ii, ArrayBlockingQueue<AyncFileMeta> queue) {
        this.queue = queue;
        this.threadId = ii;
    }

    public static AyncSender startSender(int ii, ArrayBlockingQueue<AyncFileMeta> queue) {
        AyncSender sender = new AyncSender(ii, queue);
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
                if (req == null ) {
                    req = queue.take();
                }
            } catch (InterruptedException e) {
                LOG.info("ERR:::",e);
                break;
            }
            if(req !=null) {
                LOG.info("path:::" + req.getPath());

                byte[] bs = req.getMeta();
                UploadObject uploadObject;

                Map<String,String> header;

                header = SerializationUtil.deserializeMap(bs);

                try {
                    uploadObject = new UploadObject(req.getPath());

                    uploadObject.upload();
                    ObjectHandler.createObject(req.getBucketname(), req.getKey(), uploadObject.getVNU(), bs);

                } catch (IOException | ServiceException | InterruptedException e) {
                    LOG.info("ERR:",e);
                }

                LOG.info(req.getKey() + " uploaded successfully................");
                //删除缓存文件
                LOG.info("Delete ******* CACHE FILE...........");
                Path obj = Paths.get(req.getPath());
                String xmlMeta = header.get("xmlMeta");
                Path xml = Paths.get(xmlMeta);
                try {
                    AyncUploadSenderPool.notice(req);
                    Files.delete(xml);
                    Files.delete(obj);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            req = null;
        }
    }
}

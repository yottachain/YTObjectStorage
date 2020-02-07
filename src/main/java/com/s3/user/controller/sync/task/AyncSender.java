package com.s3.user.controller.sync.task;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import de.mindconsulting.s3storeboot.service.CosBackupService;
import de.mindconsulting.s3storeboot.util.ProgressUtil;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

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

//    @Autowired
//    private CosBackupService cosBackupService;

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
                String fileLength = header.get("contentLength");
                try {
                    if("cos".equals(req.getPath())) {
                        //执行腾讯云备份
//                //Back up to tencent cloud
                        LOG.info("req.getAesPath()===="+req.getAesPath()+"  ,buckerName=="+req.getBucketname() + " ,key===="+req.getKey());
                        String cosPath = req.getAesPath();
                        if(Files.exists(Paths.get(cosPath))) {
                            LOG.info("COS BACK UP INTG.......");
                            CosBackupService cosBackupService = new CosBackupService();
                            String etag = cosBackupService.uploadFile(req.getAesPath(),req.getBucketname(),req.getKey(),req.getCosBucket());
                            LOG.info("BACKUP COMPLETE  etag==="+etag);
                            LOG.info("Delete ******* CACHE FILE.....1......");
                            Path aesPath = Paths.get(req.getAesPath());
                            Files.delete(aesPath);
                        }

        //                //Delete Cache file
//                        LOG.info("Delete ****cos*** CACHE FILE......2.....");
//                        Path aesPath = Paths.get(req.getAesPath());
//                        Files.delete(aesPath);
                        String cosMeta = header.get("cosMeta");
                        LOG.info("cosMeta======"+cosMeta);
                        Path xml = Paths.get(cosMeta);
                        if(Files.exists(xml)) {
                            Files.delete(xml);
                        }
                    } else {
                        if("0".equals(fileLength)) {
                            ObjectId VNU = new ObjectId("000000000000000000000000");
                            ObjectHandler.createObject(req.getBucketname(), req.getKey(), VNU, bs);
                            LOG.info("[ "+req.getKey() +" ]"+ " uploaded successfully................");
                        } else {
                            uploadObject = new UploadObject(req.getPath());
                            ProgressUtil.putUploadObject(req.getBucketname(),req.getKey(),uploadObject);
                            uploadObject.upload();
                            ObjectHandler.createObject(req.getBucketname(), req.getKey(), uploadObject.getVNU(), bs);
                            LOG.info("[ "+req.getKey() +" ]"+ " uploaded successfully................");
                            String status = ProgressUtil.getUserHDDStatus();
                            if("ERR".equals(status)) {
                                ProgressUtil.removeUserHDDStatus();
                            }
                        }

                        Path obj = Paths.get(req.getPath());
                        String xmlMeta = header.get("xmlMeta");
                        Path xml = Paths.get(xmlMeta);
                        Files.delete(obj);
                        Files.delete(xml);
                        req.setPath("cos");
                        //
                        LOG.info("cos status is "+AyncUploadSenderPool.newInstance().cosBackUp);
                        if("on".equals(AyncUploadSenderPool.newInstance().cosBackUp)) {
                            AyncUploadSenderPool.putAyncFileMeta(req);
                        }

                    }
                } catch (IOException | ServiceException | InterruptedException e) {
                    if("2".equals(e.getMessage())) {
                        ProgressUtil.putUserHDDStatus("ERR");
                        AyncUploadSenderPool.notice(req);
                        LOG.error("ERR:NOT_ENOUGH_DHH");
                        e.printStackTrace();
                        break;
                    } else {
                        String status = ProgressUtil.getUserHDDStatus();
                        if("ERR".equals(status)) {
                            ProgressUtil.removeUserHDDStatus();
                        }
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        continue;
                    }

                }finally {
//                    try {
//                        Thread.sleep(60000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    ProgressUtil.removeUploadObject(req.getBucketname(),req.getKey());
                }

//                LOG.info(req.getKey() + " uploaded successfully................");
//                //Back up to tencent cloud
//                LOG.info("req.getAesPath()===="+req.getAesPath()+"  ,buckerName=="+req.getBucketname() + " ,key===="+req.getKey());
//
//                CosBackupService cosBackupService = new CosBackupService();
//                String etag = cosBackupService.uploadFile(req.getAesPath(),req.getBucketname(),req.getKey(),req.getCosBucket());
//                LOG.info("BACKUP COMPLETE，etag:::"+etag);
//                //Delete Cache file
//                LOG.info("Delete ******* CACHE FILE...........");
//



                //腾讯云备份*******************
//                Path aesPath = Paths.get(req.getAesPath());
                //腾讯云备份****************************
                    AyncUploadSenderPool.notice(req);


//                    Files.delete(obj);

                    //腾讯云备份*******************
//                    Files.delete(aesPath);
                    //腾讯云备份*******************

            }
            req = null;
        }
    }
}

package com.s3.user.controller.sync.task;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.client.v2.YTClient;
import com.ytfs.client.v2.YTClientMgr;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import de.mindconsulting.s3storeboot.service.CosBackupService;
import de.mindconsulting.s3storeboot.util.ProgressUtil;
import de.mindconsulting.s3storeboot.util.PropertiesUtil;
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
                PropertiesUtil p = new PropertiesUtil("../bin/application.properties");
                String securityEnabled = p.readProperty("s3server.securityEnabled");
                String new_publicKey = null;
//                LOG.info("cos is "+"cos".equals(req.getPath()) + "!!!!!!");
                try {
                    if("cos".equals(req.getPath())) {
                        //执行腾讯云备份
//                //Back up to tencent cloud
                        LOG.info("req.getAesPath()===="+req.getAesPath()+"  ,buckerName=="+req.getBucketname() + " ,key===="+req.getKey());
                        String cosPath = req.getAesPath();
                        if(Files.exists(Paths.get(cosPath))) {
                            LOG.info("COS BACK UP INTG.......");
                            CosBackupService cosBackupService = new CosBackupService();
                            String etag = cosBackupService.uploadFile(req);
                            LOG.info("BACKUP COMPLETE  etag==="+etag);
                            LOG.info("Delete ******* CACHE FILE.....1......");
                            Path aesPath = Paths.get(req.getAesPath());
                            Files.delete(aesPath);
                        }

                        String cosMeta = header.get("cosMeta");
                        Path xml = Paths.get(cosMeta);
                        if(Files.exists(xml)) {
                            Files.delete(xml);
                        }
                    } else {
                        if("0".equals(fileLength)) {
                            ObjectId VNU = new ObjectId("000000000000000000000000");
                           if(securityEnabled.equals("true")){
                               String publicKey = req.getPublicKey();
                               new_publicKey = publicKey.substring(publicKey.indexOf("YTA")+3);
                               YTClient client = YTClientMgr.getClient(new_publicKey);
                               client.createObjectAccessor().createObject(req.getBucketname(), req.getKey(), VNU, bs);
                           } else {
                               ObjectHandler.createObject(req.getBucketname(), req.getKey(), VNU, bs);
                           }
                            Path obj = Paths.get(req.getPath());
                            String xmlMeta = header.get("xmlMeta");
                            Path xml = Paths.get(xmlMeta);
                            LOG.info("xml======="+xml);
                            LOG.info("obj======="+obj);
                            if(Files.exists(obj)) {
                                Files.delete(obj);
                            }
                            if(Files.exists(xml)) {
                                Files.delete(xml);
                            }
                            LOG.info("[ "+req.getKey() +" ]"+ " uploaded successfully................");
                        } else {
                            LOG.info("FILE is length===="+fileLength+",HERE....................");

                            if(securityEnabled.equals("true")) {
                                String publicKey = req.getPublicKey();
                                YTClient client = YTClientMgr.getClient(publicKey);
                                uploadObject = client.createUploadObject(req.getPath());
                                ProgressUtil.putUploadObject(req.getBucketname(),req.getKey(),uploadObject);
                                uploadObject.upload();
                                client.createObjectAccessor().createObject(req.getBucketname(), req.getKey(), uploadObject.getVNU(), bs);
                            } else {
                                uploadObject = new UploadObject(req.getPath());
                                ProgressUtil.putUploadObject(req.getBucketname(),req.getKey(),uploadObject);
                                uploadObject.upload();
                                ObjectHandler.createObject(req.getBucketname(), req.getKey(), uploadObject.getVNU(), bs);
                            }
                            int num = uploadObject.getProgress();
                            if(num == 100) {
                                Path obj = Paths.get(req.getPath());
                                String xmlMeta = header.get("xmlMeta");
                                Path xml = Paths.get(xmlMeta);
                                if(Files.exists(obj)) {
                                    Files.delete(obj);
                                }
                                if(Files.exists(xml)) {
                                    Files.delete(xml);
                                }
                            }
                            LOG.info("[ "+req.getKey() +" ]"+ " uploaded successfully................");

                            String status = ProgressUtil.getUserHDDStatus();
                            if("ERR".equals(status)) {
                                ProgressUtil.removeUserHDDStatus();
                            }
                            if(!"false".equals(AyncUploadSenderPool.newInstance().cosBackUp)) {
                                req.setPath("cos");
                                AyncUploadSenderPool.putAyncFileMeta(req);
                            }
                        }
                    }
                } catch (IOException | ServiceException | InterruptedException e) {
                    LOG.info("Exception*****",e);
                    if("2".equals(e.getMessage())) {
                        ProgressUtil.putUserHDDStatus("ERR");
                        AyncUploadSenderPool.notice(req);
                        LOG.error("ERR:NOT_ENOUGH_DHH");
                        e.printStackTrace();
                        break;
                    } else {
                        String status = ProgressUtil.getUserHDDStatus();
                        LOG.info("status ************"+status);
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
                    ProgressUtil.removeUploadObject(req.getBucketname(),req.getKey());
                }
                    AyncUploadSenderPool.notice(req);
            }
            req = null;
        }
    }
}

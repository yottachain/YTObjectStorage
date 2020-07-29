package com.s3.user.controller.sync.task;

import com.ytfs.client.Configurator;
import com.ytfs.client.UploadObject;
import com.ytfs.client.Version;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.client.v2.YTClient;
import com.ytfs.client.v2.YTClientMgr;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import de.mindconsulting.s3storeboot.entities.YottaUser;
import de.mindconsulting.s3storeboot.service.CosBackupService;
import de.mindconsulting.s3storeboot.util.ProgressUtil;
import de.mindconsulting.s3storeboot.util.PropertiesUtil;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
        Version.setVersionID("1.0.0.15");
        String propertiesPath = "../bin/application.properties";
        PropertiesUtil p = new PropertiesUtil(propertiesPath);
        String securityEnabled = p.readProperty("s3server.securityEnabled");
        if(securityEnabled.equals("true")){
            Configurator cfg=new Configurator();
            //矿机列表长度(328-1000)
            cfg.setPNN(p.readProperty("s3server.PNN"));
            //每N分钟更新一次矿机列表(2-10分钟)
            cfg.setPTR("s3server.PTR");
            //上传文件时最大分片并发数(50-3000)
            cfg.setUploadShardThreadNum("s3server.uploadShardThreadNum");
            //上传文件时最大块并发数(3-500)
            cfg.setUploadBlockThreadNum("s3server.uploadBlockThreadNum");
            //上传文件时没文件最大占用5M内存(3-20)
            cfg. setUploadFileMaxMemory("s3server.uploadFileMaxMemory");
            //下载文件时最大分片并发数(50-500)
            cfg. setDownloadThread ("s3server.downloadThread");
            try{
                String certList = "../conf/cert.list";
                Path path = Paths.get(certList);
                if(!Files.exists(path)) {
                    Files.createFile(path);
                }
                YTClientMgr.init(cfg);
                List<YottaUser> list = YottaUser.read();
                if(list.size() > 0){
                    for(YottaUser user : list) {
                        YTClientMgr.newInstance(user.getUsername(),user.getPrivateKey());
                    }
                }
            }catch (Exception e) {
                LOG.info("Multiuser initialization is not a significant error...",e);
            }
        }


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
//                PropertiesUtil p = new PropertiesUtil("../bin/application.properties");
//                String securityEnabled = p.readProperty("s3server.securityEnabled");
                Map<String, String> map = SerializationUtil.deserializeMap(bs);
                map.remove("cosMeta");
                map.remove("xmlMeta");
                bs = SerializationUtil.serializeMap(map);
                String new_publicKey = null;
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
                            if(Files.exists(aesPath)){
                                Files.delete(aesPath);
                            }
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
                                LOG.info("metadata size==="+bs.length);
//                                Map<String,String> ss = SerializationUtil.deserializeMap(bs);
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

package com.s3.user.controller.sync.task;

import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AyncLoader extends Thread {

    private static final Logger LOG = Logger.getLogger(AyncLoader.class);

    JAXBContext jaxbContext;

    {
        try {
            jaxbContext = JAXBContext.newInstance(AyncFileMeta.class);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    private String SYNC_DIR;

    AyncLoader(String SYNC_DIR) {
        this.SYNC_DIR = SYNC_DIR;
    }

    @Override
    public void run() {
        LOG.info("System start initiate asynchronous file upload task...");
        LOG.info("SYNC_DIR====" + SYNC_DIR);
        Path syncDir = Paths.get(SYNC_DIR);

        String[] objectList = new File(SYNC_DIR).list();
        LOG.info("File count::::" + objectList.length);
        if(objectList.length > 0) {
            for(int i=0;i<objectList.length;i++) {
                File file = new File(SYNC_DIR+"/"+objectList[i]);
                String[] nameList = file.list(((dir, name) -> name.endsWith(".xml") || new File(name).isDirectory()));
                if(nameList != null && nameList.length > 0 ) {
                    //解析XML文件
                    for(int ii=0;ii<nameList.length;ii++) {
                        String metaPath = file.getPath()+"/"+nameList[ii];
                        Path meta = Paths.get(metaPath);
                        AyncFileMeta fileMeta = loadAyncFileMeta(meta);
                        boolean b = AyncUploadSenderPool.addAyncFileMeta(fileMeta);
                        if (b) {
                            LOG.info("ok");
                        }
                    }
                }
                LOG.info("******************************");

            }
        }

//        List<AyncFileMeta> list = new ArrayList();
//        if (objectList.length > 0) {
//            for (int i = 0; i < objectList.length; i++) {
//                String filePath = syncDir.toString() + "/" + objectList[i];
//                AyncFileMeta meta = AyncFileMeta.load(filePath);
//                LOG.info("File path is ::::" + filePath);
//                list.add(meta);
//            }
//        }
//        while (!list.isEmpty()) {
//            AyncFileMeta meta = list.get(0);
//            boolean b = AyncUploadSenderPool.addAyncFileMeta(meta);
//            if (b) {
//                list.remove(meta);
//            }
//        }

    }

    //异步上传，读meta
    private AyncFileMeta loadAyncFileMeta(Path meta) {

        try (InputStream in = Files.newInputStream(meta)) {
            AyncFileMeta metaData = (AyncFileMeta) jaxbContext.createUnmarshaller().unmarshal(in);


            in.close();
            return metaData;
        } catch (IOException | JAXBException e) {
            LOG.warn("error reading meta file at " + meta.toString(), e);
        }
        return null;
    }
}

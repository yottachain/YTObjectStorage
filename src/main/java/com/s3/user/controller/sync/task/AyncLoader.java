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
    private String cosBackUp;

    AyncLoader(String SYNC_DIR,String cosBackUp) {
        this.SYNC_DIR = SYNC_DIR;
        this.cosBackUp = cosBackUp;
    }

    @Override
    public void run() {
        LOG.info("System start initiate asynchronous file upload task...");
//        Path syncDir = Paths.get(SYNC_DIR+"/"+"xml");

        String[] objectList = new File(SYNC_DIR+"/"+"xml").list();
        String[] xmlObjectList = new File(SYNC_DIR + "/cos_xml").list();
        String xmlPath = SYNC_DIR + "/" + "xml";
        String cosPath = SYNC_DIR + "/" + "cos_xml";
        if(objectList == null && xmlObjectList == null) {
            return;
        }
        if(objectList.length > 0) {
            for(int i=0;i<objectList.length;i++) {
                File file = new File(SYNC_DIR+"/"+"xml"+"/"+objectList[i]);
                Path meta = Paths.get(file.getPath());
                AyncFileMeta fileMeta = loadAyncFileMeta(meta);
                boolean b = AyncUploadSenderPool.addAyncFileMeta(fileMeta);
                if (b) {
                    LOG.info("ok");
                }
                LOG.info("******************************");

            }
            if(!"false".equals(cosBackUp)) {
                addAyncPool(xmlPath,cosPath);
            }

        } else {
            if(!"false".equals(cosBackUp)) {
                addAyncPool(xmlPath,cosPath);
            }
        }

    }

    //cos文件加入线程池
    private  void addAyncPool(String xmlPath,String cosXml) {
//        String[] objectList = new File(xmlPath).list();
        String[] cos_List = new File(cosXml).list();
        if(cos_List != null) {
            for(int i=0;i<cos_List.length;i++) {
                File file = new File(cosXml + "/" + cos_List[i]);
                Path meta = Paths.get(file.getPath());
                AyncFileMeta fileMeta = loadAyncFileMeta(meta);
                fileMeta.setPath("cos");
                boolean b = AyncUploadSenderPool.addAyncFileMeta(fileMeta);
                if (b) {
                    LOG.info("ok");
                }
            }
        }
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

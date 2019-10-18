package com.s3.user.controller.sync.task;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class AyncLoader extends Thread {

    private static final Logger LOG = Logger.getLogger(AyncLoader.class);

    private String SYNC_DIR;

    AyncLoader(String SYNC_DIR) {
        this.SYNC_DIR = SYNC_DIR;
    }

    @Override
    public void run() {
        LOG.info("【系统启动】初始化异步文件上传任务...");
        LOG.info("SYNC_DIR====" + SYNC_DIR);
        Path syncDir = Paths.get(SYNC_DIR);

        String[] objectList = new File(SYNC_DIR).list();//加过滤       
        LOG.info("File count::::" + objectList.length);

        List<AyncFileMeta> list = new ArrayList();
        if (objectList.length > 0) {
            for (int i = 0; i < objectList.length; i++) {
                String filePath = syncDir.toString() + "/" + objectList[i];
                AyncFileMeta meta = AyncFileMeta.load(filePath);
                LOG.info("File path is ::::" + filePath);
                list.add(meta);
            }
        }
        while (!list.isEmpty()) {
            AyncFileMeta meta = list.get(0);
            boolean b = SyncUploadSenderPool.addSender(meta);
            if (b) {
                list.remove(meta);
            }
        }

    }
}

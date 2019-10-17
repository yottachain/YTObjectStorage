package com.s3.user.controller.sync.task;
import com.ytfs.service.packet.s3.UploadFileReq;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class SyncUploadSenderPool {

    private static final Logger LOG = Logger.getLogger(SyncUploadSenderPool.class);

    String SYNC_DIR;
    String syncBucketName;
    int syncCount;
    private static SYNCSender[] senders;
    private static ArrayBlockingQueue<UploadFileReq> queue;
    private static SyncUploadSenderPool instance=null;
    public static SyncUploadSenderPool newInstance(){
        return instance;
    }
    public static void init(String SYNC_DIR,String syncBucketName,int syncCount){
        if(instance==null){
            instance=new SyncUploadSenderPool();
            instance.SYNC_DIR=SYNC_DIR;
            instance.syncBucketName=syncBucketName;
            instance.syncCount=syncCount;
            instance.start();
        }

    }




    public final void start() {
        //程序稳定后 队列长度可配置
        queue = new ArrayBlockingQueue(50);
        int count = syncCount;
        senders = new SYNCSender[count];
        for (int i=0;i<count;i++) {
            SYNCSender sender = SYNCSender.startSender(i,queue);
            senders[i] = sender;
        }
    }

    public final void stop() {
        List<SYNCSender> list = new ArrayList(Arrays.asList(senders));
        list.stream().forEach((sender) -> {
            sender.stopSend();
        });
    }

    public static void startSender(UploadFileReq req) {
        SYNCSender.putMessage(queue,req);
    }
}

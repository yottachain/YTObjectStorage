package com.s3.user.controller.sync.task;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AyncUploadSenderPool {

    private static final Logger LOG = Logger.getLogger(AyncUploadSenderPool.class);

    String SYNC_DIR;
    int queueSize;
    int syncCount;
    String cosBackUp;
    private AyncSender[] senders;
    static ArrayBlockingQueue<AyncFileMeta> queue;
    private static AyncUploadSenderPool instance = null;
    static List<SyncNotice> notices= Collections.synchronizedList(new ArrayList());

    public static void addSyncNotice(SyncNotice n){
        notices.add(n);
    }

    public static void notice(AyncFileMeta req){
        List<SyncNotice> ls=new ArrayList(notices);
        for(SyncNotice n:ls){
            n.removeNotice(req);
        }
    }

    public static AyncUploadSenderPool newInstance() {
        return instance;
    }

    public static void init(String SYNC_DIR, int queueSize, int syncCount,String cosBackUp) {
        if (instance == null) {
            instance = new AyncUploadSenderPool();

            /*
             1.检查SYNC_DIR是否存在，
             2.queueSize 控制最大最小值
             3.syncCount 控制最大最小值
             */
            //         if (!Files.exists(syncDir)) {
            //  Files.createDirectories(syncDir);
            //  }
            instance.SYNC_DIR = SYNC_DIR;
            instance.queueSize = queueSize;
            instance.syncCount = syncCount;
            instance.cosBackUp = cosBackUp;
            instance.start();

            AyncLoader loader = new AyncLoader(instance.SYNC_DIR,instance.cosBackUp);
            loader.start();
        }
    }

    public final void start() {
        queue = new ArrayBlockingQueue(queueSize);
        int count = syncCount;
        senders = new AyncSender[count];
        for (int i = 0; i < count; i++) {
            AyncSender sender = AyncSender.startSender(i, queue);
            senders[i] = sender;
        }
    }

    public final void stop() {
        List<AyncSender> list = new ArrayList(Arrays.asList(senders));
        list.stream().forEach((sender) -> {
            sender.stopSend();
        });
    }

    public static void putAyncFileMeta(AyncFileMeta req)  {
        try {
            LOG.info("ADD QUEUE..........");
            while(!newInstance().queue.offer(req,30,TimeUnit.SECONDS)) {
                LOG.warn("QUEUE IS FULL...");
            }
        } catch (InterruptedException  ex) {
            LOG.info("err::",ex);
        }

    }

    public static boolean addAyncFileMeta(AyncFileMeta req) {
        try {
            return newInstance().queue.offer(req, 1, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    public static boolean isFull() {

        return newInstance().queue.remainingCapacity() == 0;
    }



}

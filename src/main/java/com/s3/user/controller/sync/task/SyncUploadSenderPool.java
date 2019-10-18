package com.s3.user.controller.sync.task;

import java.io.IOException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SyncUploadSenderPool {

    private static final Logger LOG = Logger.getLogger(SyncUploadSenderPool.class);

    String SYNC_DIR;
    int queueSize;
    int syncCount;
    private SYNCSender[] senders;
    private ArrayBlockingQueue<AyncFileMeta> queue;
    private static SyncUploadSenderPool instance = null;

    public static SyncUploadSenderPool newInstance() {
        return instance;
    }

    public static void init(String SYNC_DIR, int queueSize, int syncCount) {
        if (instance == null) {
            instance = new SyncUploadSenderPool();

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
            instance.start();

            AyncLoader loader = new AyncLoader(instance.SYNC_DIR);
            loader.start();
        }
    }

    public final void start() {
        //程序稳定后 队列长度可配置
        queue = new ArrayBlockingQueue(queueSize);
        int count = syncCount;
        senders = new SYNCSender[count];
        for (int i = 0; i < count; i++) {
            SYNCSender sender = SYNCSender.startSender(i, queue);
            senders[i] = sender;
        }
    }

    public final void stop() {
        List<SYNCSender> list = new ArrayList(Arrays.asList(senders));
        list.stream().forEach((sender) -> {
            sender.stopSend();
        });
    }

    public static void putAyncFileMeta(AyncFileMeta req) throws IOException {
        req.save();
        try {
            newInstance().queue.put(req);
        } catch (InterruptedException ex) {
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

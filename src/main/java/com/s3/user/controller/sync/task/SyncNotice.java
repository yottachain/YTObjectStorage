package com.s3.user.controller.sync.task;

import java.util.ArrayList;
import java.util.List;

public class SyncNotice {


    private List<AyncFileMeta> list=  new ArrayList();


    public SyncNotice(){
        list.addAll(AyncUploadSenderPool.queue);
    }

    public void removeNotice(AyncFileMeta meta){
        synchronized(list){
            list.remove(meta);
            list.notify();
        }
    }

    public void waitComplete() {
        synchronized(list){
            while(!list.isEmpty()){
                try {
                    list.wait(1000*5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            AyncUploadSenderPool.notices.remove(this);
        }
    }


}

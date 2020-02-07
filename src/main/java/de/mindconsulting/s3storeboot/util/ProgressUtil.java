package de.mindconsulting.s3storeboot.util;

import com.ytfs.client.UploadObject;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by phs on 2020/1/17.
 */
public class ProgressUtil {

    //Key: bucket+"-"+key
    private static ConcurrentHashMap<String,UploadObject> cacheMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String,String> hddMap = new ConcurrentHashMap<>();
    public static void putUploadObject(String bucket,String key,UploadObject object){
        String s= bucket+"-"+key;
        String sha256 = SHA256Util.getSHA256(s);
        cacheMap.put(sha256,object);
    }
    public static void putUserHDDStatus(String status){
        hddMap.put("HDD",status);
    }

    public static void removeUserHDDStatus() {
        hddMap.remove("HDD");
    }

    public static String getUserHDDStatus() {
        String status = hddMap.get("HDD");
        return status;
    }

    public static void removeUploadObject(String bucket,String key){
        String s= bucket+"-"+key;
        String sha256 = SHA256Util.getSHA256(s);
        cacheMap.remove(sha256);
    }

    public static int getProgress(String sha256){
//        String s= bucket+"-"+key;
        UploadObject object=cacheMap.get(sha256);
        if(object == null) {
            return 0;
        }else if(sha256==null){
            return 100;
        }else{
            return object.getProgress();
        }
    }

//    public static void main(String[] args) {
//        testMap.put("sss","222");
//        String str1 = testMap.get("sss");
//        System.out.println(str1);
//        testMap.put("sss","test");
//        String str2 = testMap.get("sss");
//        System.out.println(str2);
//    }
}

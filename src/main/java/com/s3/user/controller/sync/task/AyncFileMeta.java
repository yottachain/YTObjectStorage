package com.s3.user.controller.sync.task;

import com.ytfs.common.SerializationUtil;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class AyncFileMeta {

    private String key;
    private String bucketname;
    private byte[] meta;
    private String path;

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return the bucketname
     */
    public String getBucketname() {
        return bucketname;
    }

    /**
     * @param bucketname the bucketname to set
     */
    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    /**
     * @return the meta
     */
    public byte[] getMeta() {
        return meta;
    }

    /**
     * @param meta the meta to set
     */
    public void setMeta(byte[] meta) {
        this.meta = meta;
    }

    public void save() throws IOException {
        byte[] bs = SerializationUtil.serializeNoID(this);
        String fileName = path + ".met";

        OutputStream os = new FileOutputStream(fileName);
        os.write(bs);
        os.close();

    }

    public static AyncFileMeta load(String path)   {
//********************ci
        byte[] bs = null;
        AyncFileMeta ins = new AyncFileMeta();
        SerializationUtil.deserializeNoID(bs, ins);
        return ins;
    }

    /**
     * @return the path
     */
    public String getPath() {
        return path;
    }

    /**
     * @param path the path to set
     */
    public void setPath(String path) {
        this.path = path;
    }

}

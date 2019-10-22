package com.s3.user.controller.sync.task;

import com.ytfs.common.SerializationUtil;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "AyncFileMeta")
public class AyncFileMeta {

    private String key;
    private String bucketname;
    private byte[] meta;
    private String path;

    /**
     * @return the key
     */
    @XmlElement(name = "key")
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
    @XmlElement(name = "bucketname")
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
    @XmlElement(name = "meta")
    public byte[] getMeta() {
        return meta;
    }

    /**
     * @param meta the meta to set
     */
    public void setMeta(byte[] meta) {
        this.meta = meta;
    }

//    public void save() throws IOException {
//        byte[] bs = SerializationUtil.serializeNoID(this);
//        String fileName = path + ".met";
//
//        OutputStream os = new FileOutputStream(fileName);
//        os.write(bs);
//        os.close();
//
//    }

    public static AyncFileMeta load(String path)   {
//********************ci
        byte[] bs = null;
        AyncFileMeta file = new AyncFileMeta();
        file.setBucketname("bucket-test-01");
        file.setKey("testtt");
        file.setPath("D:/s3cache/sync");
        SerializationUtil.deserializeNoID(bs, file);
        file.setMeta(bs);
        return file;
    }

    /**
     * @return the path
     */
    @XmlElement(name = "path")
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

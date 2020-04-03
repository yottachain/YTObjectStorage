package com.s3.user.controller.sync.task;

import de.mc.ladon.s3server.entities.api.S3CallContext;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "AyncFileMeta")
public class AyncFileMeta {

    private String key;
    private String bucketname;
    private byte[] meta;
    private String path;
    private String aesPath;
    private String cosBucket;
    private String publicKey;


    @XmlElement(name = "publicKey")
    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

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

//    public static AyncFileMeta load(String path)   {
//        byte[] bs = null;
//        AyncFileMeta file = new AyncFileMeta();
//        file.setBucketname("bucket-test-01");
//        file.setKey("testtt");
//        file.setPath("D:/s3cache/sync");
//        SerializationUtil.deserializeNoID(bs, file);
//        file.setMeta(bs);
//        return file;
//    }

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

    @XmlElement(name = "aesPath")
    public String getAesPath() {
        return aesPath;
    }

    public void setAesPath(String aesPath) {
        this.aesPath = aesPath;
    }

    public String getCosBucket() {
        return cosBucket;
    }
    @XmlElement(name = "aesBucket")
    public void setCosBucket(String cosBucket) {
        this.cosBucket = cosBucket;
    }
}

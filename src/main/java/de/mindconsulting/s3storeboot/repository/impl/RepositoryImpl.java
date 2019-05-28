package de.mindconsulting.s3storeboot.repository.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.ytfs.client.DownloadObject;
import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.BucketHandler;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import de.mc.ladon.s3server.common.Encoding;
import de.mc.ladon.s3server.common.S3Constants;
import de.mc.ladon.s3server.common.StreamUtils;
import de.mc.ladon.s3server.entities.api.*;
import de.mc.ladon.s3server.entities.impl.*;
import de.mc.ladon.s3server.exceptions.*;
import de.mc.ladon.s3server.repository.api.S3Repository;
import de.mindconsulting.s3storeboot.jaxb.meta.StorageMeta;
import de.mindconsulting.s3storeboot.jaxb.meta.UserData;
import de.mindconsulting.s3storeboot.util.S3Lock;
import org.bson.types.ObjectId;
import org.omg.CORBA.ObjectHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class RepositoryImpl implements S3Repository {
    public static final String META_XML_EXTENSION = "_meta.xml";
    public static final String USER_FOLDER = "users";
    public static final String SYSTEM_DEFAULT_USER = "SYSTEM";
    private Logger logger = LoggerFactory.getLogger(RepositoryImpl.class);
    private static final String DATA_FOLDER = "data";
    private static final String META_FOLDER = "meta";
    private final String repoBaseUrl;
    private final JAXBContext jaxbContext;
    private final ConcurrentMap<String, S3User> userMap;
    public static final Predicate<Path> IS_DIRECTORY = p -> Files.isDirectory(p);
    public final String accessKey;
    private final int allowMaxSize;


    public RepositoryImpl(String repoBaseUrl,String accessKey,int allowMaxSize) {
        this.repoBaseUrl = repoBaseUrl;
        this.accessKey = accessKey;
        this.allowMaxSize = allowMaxSize;
        try {
            jaxbContext = JAXBContext.newInstance(StorageMeta.class, UserData.class);
            userMap = new ConcurrentHashMap<>(loadUserFile());
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }


    private Map<String, S3StoreUser> loadUserFile() {
        Path basePath = Paths.get(repoBaseUrl);
        Path userFile = basePath.resolve("userdb" + META_XML_EXTENSION);
        try {
            if (!Files.exists(userFile)) {
                initDefaultUser();
            }
            try (InputStream in = Files.newInputStream(userFile)) {
                UserData userData = (UserData) jaxbContext.createUnmarshaller().unmarshal(in);
                return userData.getUsers();
            }
        } catch (IOException | JAXBException e) {
            logger.error("error reading user file ", e);
            throw new RuntimeException(e);
        }

    }

    private void initDefaultUser() throws IOException, JAXBException {
        Path basePath = Paths.get(repoBaseUrl);
        Files.createDirectories(basePath);
        Path userFile = basePath.resolve("userdb" + META_XML_EXTENSION);
        try (OutputStream out = Files.newOutputStream(userFile)) {
            Marshaller m = jaxbContext.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
//            m.marshal(new UserData(ImmutableMap.of(accessKey, new S3StoreUser(accessKey, accessKey, accessKey, accessKey, null))), out);
           logger.info("+++++++++++++++initDefaultUser()");
            m.marshal(new UserData(ImmutableMap.of(accessKey, new S3StoreUser(accessKey, accessKey, accessKey, accessKey, null))), out);

        }

    }

    //返回当前用户链上的所有bucket
    @Override
    public List<S3Bucket> listAllBuckets(S3CallContext callContext) {

        List<S3Bucket> s3Buckets = new ArrayList<>();
        try {
            System.out.println("listAllBuckets=========================");
            String[] buckets = BucketHandler.listBucket();
            System.out.println("buckets========================="+buckets.length);
            for(int i=0;i<buckets.length;i++)  {
                S3BucketImpl  s3Bucket = new S3BucketImpl(buckets[i],new Date(),callContext.getUser());
                s3Buckets.add(s3Bucket);
            }

        } catch (ServiceException e) {
            e.printStackTrace();
        }
        return s3Buckets;

    }

    @Override
    public void createBucket(S3CallContext callContext, String bucketName, String locationConstraint) {
        Path dataBucket = Paths.get(repoBaseUrl, bucketName, DATA_FOLDER);
        Path metaBucket = Paths.get(repoBaseUrl, bucketName, META_FOLDER);
        Path metaBucketFile = Paths.get(repoBaseUrl, bucketName + META_XML_EXTENSION);


        try {
            Files.createDirectories(dataBucket);
            Files.createDirectories(metaBucket);
            byte[] bs = writeMetaFile(metaBucketFile, callContext);

            //本地配置路径下创建完bucket之后，调用ytfs创建bucket方法
            logger.info("开始调用BucketHandler.createBucket（）:===========");
            boolean isBucketExist = this.checkBucketExist(bucketName);
            if(isBucketExist == false) {
                BucketHandler.createBucket(bucketName,bs);
            }
            logger.info("创建bucket完毕");

        } catch (IOException | JAXBException | ServiceException e) {
            logger.error("internal error", e);
            throw new InternalErrorException(bucketName, callContext.getRequestId());
        }
    }

    private byte[] writeMetaFile(Path meta, S3CallContext callContext, String... additional) throws IOException, JAXBException {
        Map<String, String> header = callContext.getHeader().getFullHeader();
        header.put("contentLength",callContext.getHeader().getContentLength().toString());
        if (additional.length > 0 && additional.length % 2 == 0) {
            for (int i = 0; i < additional.length; i = i + 2) {
                header.put(additional[i], additional[i + 1]);
            }
        }
        byte[] bs = SerializationUtil.serializeMap(header);
        System.out.println("byte长度："+bs.length);
        header = SerializationUtil.deserializeMap(bs);
        Files.createDirectories(meta.getParent());
        try (OutputStream out = Files.newOutputStream(meta)) {
            Marshaller m = jaxbContext.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            m.marshal(new StorageMeta(header), out);
        }
        return bs;
    }


    @Override
    public void deleteBucket(S3CallContext callContext, String bucketName) {
        //1、判断bucket是否存在
        boolean isExistBucket = this.checkBucketExist(bucketName);
        if(isExistBucket == false) {
            throw new NoSuchBucketException(bucketName, callContext.getRequestId());
        }

        //2、如果bucket存在判断bucket下是否存在文件
        Map<String,byte[]> map = new HashMap<>();
        try {
             Map<ObjectId,String> lastMap = ObjectHandler.listObject(map,bucketName,null,1000);
             if(lastMap.size()>0) {
                 new BucketNotEmptyException(bucketName, callContext.getRequestId());
             }
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        if(map.size() > 0 ) {
            new BucketNotEmptyException(bucketName, callContext.getRequestId());
        }
        //3、调用删除接口
        try {
            BucketHandler.deleteBucket(bucketName);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createObject(S3CallContext callContext, String bucketName, String objectKey) {

        //1、判断链上是否存在此bucket
        boolean isBucketExist = this.checkBucketExist(bucketName);
        if(!isBucketExist) {
            throw new NoSuchBucketException(bucketName, callContext.getRequestId());
        }
        Path dataBucket = Paths.get(repoBaseUrl, bucketName, DATA_FOLDER);
        Path metaBucket = Paths.get(repoBaseUrl, bucketName, META_FOLDER);
        if (!Files.exists(dataBucket)){
            try {
                Files.createDirectories(dataBucket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(!Files.exists((metaBucket))) {
            try {
                Files.createDirectories(metaBucket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        Path obj = dataBucket.resolve(objectKey);
        Path meta = metaBucket.resolve(objectKey + META_XML_EXTENSION);
        Long contentLength = callContext.getHeader().getContentLength();
        String md5 = callContext.getHeader().getContentMD5();

//            lock(metaBucket, objectKey, S3Lock.LockType.write, callContext);
        try (InputStream in = callContext.getContent()) {
            if (!Files.exists(obj)) {
                Files.createDirectories(obj.getParent());
                Files.createFile(obj);
            }

            DigestInputStream din = new DigestInputStream(in, MessageDigest.getInstance("MD5"));
            try (OutputStream out = Files.newOutputStream(obj)) {
                long bytesCopied = StreamUtils.copy(din, out);
                byte[] md5bytes = din.getMessageDigest().digest();
                String storageMd5base64 = BaseEncoding.base64().encode(md5bytes);
                String storageMd5base16 = Encoding.toHex(md5bytes);

                if (contentLength != null && contentLength != bytesCopied
                        || md5 != null && !md5.equals(storageMd5base64)) {
                    Files.delete(obj);
                    Files.deleteIfExists(meta);
                    throw new InvalidDigestException(objectKey, callContext.getRequestId());
                }

                S3ResponseHeader header = new S3ResponseHeaderImpl();
                header.setEtag(inQuotes(storageMd5base16));
                header.setDate(new Date(Files.getLastModifiedTime(obj).toMillis()));
                callContext.setResponseHeader(header);

                Files.createDirectories(meta.getParent());
                byte[] metaByte = null;
                metaByte = writeMetaFile(meta, callContext, S3Constants.ETAG, inQuotes(storageMd5base16));
                //判断文件在链上是否存在，默认不存在
                boolean isFileExist = false;
                try {
                    isFileExist = ObjectHandler.isExistObject(bucketName,objectKey);
                } catch (ServiceException e) {
                    e.printStackTrace();
                }

                // 如果文件不存在  将文件上传至超级节点
                if (isFileExist == false) {
                    String filePath = obj.toString();
                    UploadObject uploadObject = new UploadObject(filePath);
                    try {
                        uploadObject.upload();
                        ObjectHandler.createObject(bucketName, objectKey, uploadObject.getVNU(), metaByte);
                        System.out.println("上传成功");
                        isFileExist = true;

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else {
                    logger.error("文件在链上已经存在或者文件名重复");
//                    throw new InternalErrorException(objectKey, callContext.getRequestId());
                }
            }

        } catch (IOException | NoSuchAlgorithmException | JAXBException e) {
            logger.error("internal error", e);
//            throw new InternalErrorException(objectKey, callContext.getRequestId());
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.error("interrupted thread", e);
//            throw new InternalErrorException(objectKey, callContext.getRequestId());
            e.printStackTrace();
        } finally {
            unlock(metaBucket, objectKey, callContext);

        }
    }


    private void unlock(Path metaPath, String objectKey, S3CallContext callContext) {
        S3Lock lock;
        try {
            lock = S3Lock.load(metaPath, objectKey);
            if (lock.isUnlockAllowed(callContext.getUser())) {
                lock.delete(callContext.getUser(), metaPath, objectKey);
            } else {
                throw new OperationAbortedException(objectKey, callContext.getRequestId());
            }
        } catch (IOException e) {
            logger.debug("resource not locked", e);
        }
    }

    private String inQuotes(String etag) {
        return "\"" + etag + "\"";
    }

    private void lock(Path metaPath, String objectKey, S3Lock.LockType lockType, S3CallContext callContext) {
        S3Lock lock;
        try {
            if (Files.exists(S3Lock.getPath(metaPath, objectKey))) {
                lock = S3Lock.load(metaPath, objectKey);
                if (!lock.isObsolete()) {
                    throw new OperationAbortedException(objectKey, callContext.getRequestId());
                }
            }
            lock = new S3Lock(lockType, callContext.getUser());
            lock.save(metaPath, objectKey);
        } catch (IOException e) {
            throw new InternalErrorException(objectKey, callContext.getRequestId());
        }
    }

    @Override
    public S3Object copyObject(S3CallContext callContext, String srcBucket, String srcObjectKey, String destBucket, String destObjectKey, boolean copyMetadata) {
        Path srcBucketData = Paths.get(repoBaseUrl, srcBucket, DATA_FOLDER);
        Path srcBucketMeta = Paths.get(repoBaseUrl, srcBucket, META_FOLDER);
        if (!Files.exists(srcBucketData))
            throw new NoSuchBucketException(srcBucket, callContext.getRequestId());
        Path srcObject = srcBucketData.resolve(srcObjectKey);
        Path srcObjectMeta = srcBucketMeta.resolve(srcObjectKey + META_XML_EXTENSION);

        Path destBucketData = Paths.get(repoBaseUrl, destBucket, DATA_FOLDER);
        Path destBucketMeta = Paths.get(repoBaseUrl, destBucket, META_FOLDER);
        if (!Files.exists(destBucketData))
            throw new NoSuchBucketException(destBucket, callContext.getRequestId());
        Path destObject = destBucketData.resolve(destObjectKey);
        Path destObjectMeta = destBucketMeta.resolve(destObjectKey + META_XML_EXTENSION);


        if (!Files.exists(srcObject))
            throw new NoSuchKeyException(srcObjectKey, callContext.getRequestId());

        lock(srcBucketMeta, srcObjectKey, S3Lock.LockType.read, callContext);
        lock(destBucketMeta, destObjectKey, S3Lock.LockType.write, callContext);

        S3Metadata srcMetadata = loadMetaFile(srcObjectMeta);
        try (InputStream in = Files.newInputStream(srcObject)) {
            if (!Files.exists(destObject)) {
                Files.createDirectories(destObject.getParent());
                Files.createFile(destObject);
            }
            try (OutputStream out = Files.newOutputStream(destObject)) {
                long bytesCopied = StreamUtils.copy(in, out);
                if (copyMetadata) {
                    if (Files.exists(srcObjectMeta)) {
                        writeMetaFile(destObjectMeta, callContext, toArray(srcMetadata));
                    }
                } else {
                    writeMetaFile(destObjectMeta, callContext);
                }
            }
            S3Metadata destMetadata = loadMetaFile(destObjectMeta);
            return new S3ObjectImpl(destObjectKey,
                    new Date(destObject.toFile().lastModified()),
                    destBucket, destObject.toFile().length(),
                    new S3UserImpl(),
                    destMetadata,
                    null, destMetadata.get(S3Constants.CONTENT_TYPE),
                    destMetadata.get(S3Constants.ETAG), destMetadata.get(S3Constants.VERSION_ID), false, true);
        } catch (IOException | JAXBException e) {
            logger.error("internal error", e);
            throw new InternalErrorException(destObjectKey, callContext.getRequestId());
        } catch (InterruptedException e) {
            logger.error("interrupted thread", e);
            throw new InternalErrorException(destObjectKey, callContext.getRequestId());
        } finally {
            unlock(srcBucketMeta, srcObjectKey, callContext);
            unlock(destBucketMeta, destObjectKey, callContext);
        }

    }

    private String[] toArray(S3Metadata srcObjectMeta) {
        List<String> result = new ArrayList<String>();
        ((S3MetadataImpl) srcObjectMeta).forEach((k, v) -> {
            result.add(k);
            result.add(v);
        });
        return result.toArray(new String[0]);
    }

    public boolean checkBucketExist(String bucketName){
        boolean flag = false;
        try {
            String[] buckets = BucketHandler.listBucket();
            if(!Arrays.asList(buckets).contains(bucketName)) {
                flag = false;
            } else {
                flag = true;
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        return flag;
    }

    @Override
    public void getObject(S3CallContext callContext, String bucketName, String objectKey, boolean head) {

        boolean isBucketExist = this.checkBucketExist(bucketName);
        if(!isBucketExist) {
            throw new NoSuchBucketException(bucketName, callContext.getRequestId());
        }
        //isObjectExist 为true,表示可以从链上获取到文件，为false则说明当前文件在当前bucket下不存在
        boolean isObjectExist = false;
        try {
            isObjectExist = ObjectHandler.isExistObject(bucketName,objectKey);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        if(isObjectExist == false) {
            throw new NoSuchKeyException(objectKey, callContext.getRequestId());
        }
        //防止链上有bucket，本地没有，下载文件之前提前创建，目的是从链上拿到的文件先放至缓存中
        Path dataBucket = Paths.get(repoBaseUrl, bucketName, DATA_FOLDER);
        Path metaBucket = Paths.get(repoBaseUrl, bucketName, META_FOLDER);
        if (!Files.exists(dataBucket)){
            try {
                Files.createDirectories(dataBucket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(!Files.exists((metaBucket))) {
            try {
                Files.createDirectories(metaBucket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        DownloadObject obj= null;
        try {
            obj = new DownloadObject(bucketName, objectKey);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        InputStream is=obj.load();
        try {
            S3ResponseHeader header = new S3ResponseHeaderImpl();
            header.setContentLength(obj.getLength());
            header.setContentType("application/octetstream");
            callContext.setResponseHeader(header);
            if (!head)
                callContext.setContent(is);
        } catch (Exception e) {
            logger.error("internal error", e);
            e.printStackTrace();
        }
    }

    private static void writeToLocal(String destination, InputStream input,String fileName)
            throws IOException {
        int index;
        byte[] bytes = new byte[1024];
        FileOutputStream downloadFile = new FileOutputStream(destination+"/"+fileName);
        while ((index = input.read(bytes)) != -1) {
            downloadFile.write(bytes, 0, index);
            downloadFile.flush();
        }
        downloadFile.close();
        input.close();
    }


    private void loadMetaFileIntoHeader(Path meta, S3CallContext callContext) {
        S3Metadata userMetadata = loadMetaFile(meta);
        S3ResponseHeader header = new S3ResponseHeaderImpl(userMetadata);
        callContext.setResponseHeader(header);
    }

    private S3Metadata loadMetaFile(Path meta) {
        try (InputStream in = Files.newInputStream(meta)) {
            StorageMeta metaData = (StorageMeta) jaxbContext.createUnmarshaller().unmarshal(in);
            return new S3MetadataImpl(metaData.getMeta());
        } catch (IOException | JAXBException e) {
            logger.warn("error reading meta file at " + meta.toString(), e);
        }
        return new S3MetadataImpl();
    }
    private S3Metadata getMetaMessage(Map<String,String> header) {
        return new S3MetadataImpl(header);
    }

    private String getMimeType(Path path) throws IOException {
        String mime = Files.probeContentType(path);
        return mime != null ? mime : "application/octet-stream";
    }

    @Override
    public S3ListBucketResult listBucket(S3CallContext callContext, String bucketName) {
        Integer maxKeys = callContext.getParams().getMaxKeys();
        String marker = callContext.getParams().getMarker();
        ObjectId lastId=null;
        String fileName = null;
        if(marker != null) {
            fileName = marker;
            try {
                lastId = ObjectHandler.getObjectIdByName(bucketName,fileName);
            } catch (ServiceException e) {
                e.printStackTrace();
            }
        }

        List<S3Object> objects = new ArrayList<>();
        try {
            Map<String,byte[]> map=new TreeMap<>();
            Map<ObjectId,String> lastMap = new HashMap<>();
            lastMap = ObjectHandler.listObject(map,bucketName,lastId,maxKeys);
            if(lastMap.size() == 1) {
                for(Map.Entry<ObjectId,String> entry : lastMap.entrySet()) {
                    lastId = entry.getKey();
                    fileName = entry.getValue();
                    marker = fileName;
                }
            } else {
                marker = null;
            }
            Iterator<Map.Entry<String, byte[]>> it = map.entrySet().iterator();
            if(map.size()>0) {
                while (it.hasNext()) {
                    Map.Entry<String,byte[]> entry = it.next();
                    String key = entry.getKey();
                    System.out.println("key==============="+key);
                    byte[] meta = entry.getValue();
                    Map<String, String> header = SerializationUtil.deserializeMap(meta);

                    S3Metadata s3Metadata = getMetaMessage(header);
                    long size = Long.parseLong(header.get("contentLength"));
                    objects.add(new S3ObjectImpl(
                            key,
                            new Date(),
                            bucketName,
                            size,
                            new S3UserImpl(),
                            s3Metadata,
                            null,
                            s3Metadata.get(S3Constants.CONTENT_TYPE),
                            s3Metadata.get(S3Constants.ETAG),
                            s3Metadata.get(S3Constants.VERSION_ID),
                            false,
                            true));

                }
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        System.out.println("lastFilename==="+fileName+",marker======="+marker);
        return new S3ListBucketResultImpl(objects, null, lastId!=null, bucketName, marker, null);
    }

    private Stream<Path> getPathStream(String bucketName, String prefix, Path bucket, String marker) throws IOException {
        final Boolean[] markerFound = new Boolean[]{marker == null};
        return Files.walk(Paths.get(repoBaseUrl, bucketName, DATA_FOLDER)).sorted()
                .filter(p -> {
                    String relpath = bucket.relativize(p).toString();
                    boolean include = markerFound[0];
                    if (!markerFound[0]) {
                        markerFound[0] = relpath.equals(marker);
                    }
                    return Files.isRegularFile(p)
                            && relpath.startsWith(prefix)
                            && include;
                });
    }


    @Override
    public void deleteObject(S3CallContext callContext, String bucketName, String objectKey) {
        boolean isExistBucket = this.checkBucketExist(bucketName);
        if(isExistBucket == false) {
            throw new NoSuchBucketException(bucketName, callContext.getRequestId());
        }
        boolean isExistObject = false;
        if(isExistObject) {
            throw new NoSuchKeyException(objectKey, callContext.getRequestId());
        }
        try {
            ObjectHandler.deleteObject(bucketName,objectKey);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getBucket(S3CallContext callContext, String bucketName) {
        Path bucket = Paths.get(repoBaseUrl, bucketName);
        if (!Files.exists(bucket))
            throw new NoSuchBucketException(bucketName, callContext.getRequestId());


    }

    @Override
    public S3User getUser(S3CallContext callContext, String accessKey) {
        logger.info("getuser()    accessKey======="+accessKey);
        return loadUser(callContext,accessKey);
    }

    private S3User loadUser(S3CallContext callContext, String accessKey) {
        logger.info("loadUser()    accessKey======="+accessKey);
        S3User user = userMap.get(accessKey);
        if (user != null) return user;
        else {
            S3User reloaded = loadUserFile().get(accessKey);
            if (reloaded != null) {
                return userMap.put(accessKey, reloaded);
            }
        }
        throw new InvalidAccessKeyIdException("", callContext.getRequestId());
    }
}

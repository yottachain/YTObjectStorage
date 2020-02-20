package de.mindconsulting.s3storeboot.repository.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.s3.user.controller.sync.task.AyncFileMeta;
import com.s3.user.controller.sync.task.AyncUploadSenderPool;
import com.ytfs.client.DownloadObject;
import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.BucketHandler;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.common.SerializationUtil;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.AESCoder;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import de.mc.ladon.s3server.common.*;
import de.mc.ladon.s3server.common.Encoding;
import de.mc.ladon.s3server.entities.api.*;
import de.mc.ladon.s3server.entities.impl.*;
import de.mc.ladon.s3server.exceptions.*;
import de.mc.ladon.s3server.jaxb.entities.*;
import de.mc.ladon.s3server.repository.api.S3Repository;
import de.mindconsulting.s3storeboot.jaxb.meta.StorageMeta;
import de.mindconsulting.s3storeboot.jaxb.meta.UserData;
import de.mindconsulting.s3storeboot.service.CosBackupService;
import de.mindconsulting.s3storeboot.util.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.w3c.dom.Document;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.System.out;

public class RepositoryImpl implements S3Repository {
    private static final Logger LOG = Logger.getLogger(RepositoryImpl.class);
    public static final String META_XML_EXTENSION = "_meta.xml";
    public static final String USER_FOLDER = "users";
    public static final String SYSTEM_DEFAULT_USER = "SYSTEM";
    private static final String DATA_FOLDER = "data";
    private static final String META_FOLDER = "meta";
    private final String repoBaseUrl;
    private final JAXBContext jaxbContext;
    private final ConcurrentMap<String, S3User> userMap;
    public static final Predicate<Path> IS_DIRECTORY = p -> Files.isDirectory(p);
    public final String accessKey;
    private final int allowMaxSize;
    private final String defaultVNU = "000000000000000000000000";
    private final String status_sync;
    private final String syncBucketName;
    private final int sync_count;
    private final String cosBucket;
    private final String cosBackUp;


    public RepositoryImpl(String repoBaseUrl,String accessKey,int allowMaxSize,String status_sync,String syncBucketName,int sync_count,String cosBucket,String cosBuckUp) {
        this.repoBaseUrl = repoBaseUrl;
        this.accessKey = accessKey;
        this.allowMaxSize = allowMaxSize;
        this.status_sync = status_sync;
        this.syncBucketName = syncBucketName;
        this.sync_count=sync_count;
        this.cosBucket = cosBucket;
        this.cosBackUp = cosBuckUp;
        try {
            jaxbContext = JAXBContext.newInstance(StorageMeta.class, UserData.class,AyncFileMeta.class);
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
            LOG.error("error reading user file ", e);
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
            m.marshal(new UserData(ImmutableMap.of(accessKey, new S3StoreUser(accessKey, accessKey, accessKey, accessKey, null))), out);

        }

    }

    //返回当前用户链上的所有bucket
    @Override
    public List<S3Bucket> listAllBuckets(S3CallContext callContext) {

        List<S3Bucket> s3Buckets = new ArrayList<>();
        try {
            String[] buckets = BucketHandler.listBucket();
            LOG.info("bucket count:::"+buckets.length);
            if (buckets.length > 0)
            for (String bucket : buckets) {
                S3BucketImpl s3Bucket = new S3BucketImpl(bucket, new Date(), callContext.getUser());
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
            Map<String,String> header = SerializationUtil.deserializeMap(bs);
            String version_status = header.get("version_status");
            if(version_status == null || "".equals(version_status)) {
                header.put("version_status","Enabled");
            }
            header.put("etag",S3Constants.ETAG);
            byte[] new_byte = SerializationUtil.serializeMap(header);


            BucketHandler.createBucket(bucketName,new_byte);
            LOG.info("***********CREATE BUCKET SUCCESS***********");

        } catch (IOException | JAXBException | ServiceException e) {
            LOG.error("ERR_MSG: ",e);
            throw new InternalErrorException(bucketName, callContext.getRequestId());
        }
    }

    @Override
    public void updateBucketVersion(S3CallContext callContext, String bucketName) {
        try {
            InputStream inputStream = callContext.getContent();
            byte[] bs = toByteArray(inputStream);
            String versionXMLMsg = new String(bs);
            Document doc = this.stringTOXml(versionXMLMsg);
            String nodePath = "/VersioningConfiguration/Status";
            String version_status = this.getNodeValue(doc,nodePath);
            Map<String,byte[]> map = new HashMap<>();
            try {
                map = BucketHandler.getBucketByName(bucketName);
            } catch (ServiceException e) {
                e.printStackTrace();
            }
            byte[] bytes = map.get(bucketName);
            Map<String,String> header = SerializationUtil.deserializeMap(bytes);
            String status = header.get("version_status");
            if(status == null || !status.equals(version_status)) {
                header.put("version_status",version_status);
                bytes = SerializationUtil.serializeMap(header);
                try {
                    BucketHandler.updateBucket(bucketName,bytes);
                } catch (ServiceException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public VersioningConfiguration getBucketVersioning(S3CallContext callContext, String bucketName) {
        Map<String,byte[]> map = new HashMap<>();
        try {
            map = BucketHandler.getBucketByName(bucketName);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        byte[] bs = map.get(bucketName);
        Map<String,String> header = SerializationUtil.deserializeMap(bs);
        String version_status = header.get("version_status");
        VersioningConfiguration versioningConfiguration = new VersioningConfiguration();
        versioningConfiguration.setStatus(version_status);
        return versioningConfiguration;
    }

    @Override
    public ListVersionsResult listVersions(S3CallContext callContext, String bucketName) {
        Integer maxKeys = callContext.getParams().getMaxKeys();
        String marker = callContext.getParams().getMarker();
        String prefix = callContext.getParams().getPrefix() != null ? callContext.getParams().getPrefix() : "";
        String delimiter = callContext.getParams().getDelimiter();
        boolean isVersion = callContext.getParams().listVersions();
        String nextVersionId = callContext.getParams().getVersionIdMarker();
        ObjectId nextId = null;
        if(nextVersionId != null) {
            nextId = new ObjectId(nextVersionId);
        } else {
            nextId = new ObjectId(defaultVNU);
        }
        String fileName = null;
        if(marker != null) {
            out.println("marker=="+marker);
            fileName = marker;
        }
        List<AbstractVersionElement> versions = new ArrayList<>();
        try {
            List<FileMetaMsg> fileMetas = ObjectHandler.listBucket(bucketName,fileName,prefix,isVersion,nextId,maxKeys);
            for(FileMetaMsg fileMeta : fileMetas) {
                Map<String,String> header = SerializationUtil.deserializeMap(fileMeta.getMeta());
                AbstractVersionElement abstractVersionElement = new AbstractVersionElement(
                        new Owner(callContext.getUser().getUserId(),callContext.getUser().getUserName()),
                        fileMeta.getFileName(),
                        fileMeta.getVersionId().toString(),
                        fileMeta.isLatest(),
                        new Date(Long.valueOf(header.get("x-amz-date"))),
                        header.get("ETag"),
                        Long.valueOf(header.get("contentLength")),
                        "STANDARD"
                );
                versions.add(abstractVersionElement);
            }

            if(fileMetas.size() < maxKeys) {
                LOG.info("");
                marker = null;
                nextVersionId = null;
            } else {
                marker = fileMetas.get(fileMetas.size()-1).getFileName();
                nextVersionId = fileMetas.get(fileMetas.size()-1).getVersionId().toString();
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        if(delimiter == null) {
            return new ListVersionsResult(callContext,bucketName,versions,null,marker != null,marker,nextVersionId);
        } else {
            if(!delimiter.equals("/"))
                throw new InvalidTokenException(delimiter,callContext.getRequestId());
            Set<String> prefixes = new HashSet<>();
            for(AbstractVersionElement version : versions) {
                String commonPrefix = DelimiterUtil.getCommonPrefix(version.getKey(),prefix,"/");
                if(commonPrefix != null) {
                    prefixes.add(commonPrefix);
                } else {
                }
            }
            List<String> prefList = new ArrayList<>(prefixes);
            return new ListVersionsResult(callContext,bucketName,versions,prefixes.isEmpty()?null: (CommonPrefixes) prefList,marker!=null,marker,nextVersionId);
        }
    }

    public static byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            byte[] b = new byte[1024];
            int n = 0;
            while ((n = is.read(b)) != -1) {
                output.write(b, 0, n);
            }
            return output.toByteArray();
        } finally {
            output.close();
        }
    }

    /**
     * @return Document 对象
     */
    public Document stringTOXml(String str) {

        StringBuilder sXML = new StringBuilder();
        sXML.append(str);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = null;
        try {
            InputStream is = new ByteArrayInputStream(sXML.toString().getBytes("utf-8"));
            doc = dbf.newDocumentBuilder().parse(is);
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return doc;
    }

    /**
     *
     * @param document
     * @return 某个节点的值 前提是需要知道xml格式，知道需要取的节点相对根节点所在位置
     */
    public String getNodeValue(Document document, String nodePaht) {
        XPathFactory xpfactory = XPathFactory.newInstance();
        XPath path = xpfactory.newXPath();
        String servInitrBrch = "";
        try {
            servInitrBrch = path.evaluate(nodePaht, document);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
        return servInitrBrch;
    }


    private byte[] writeMetaFile(Path meta, S3CallContext callContext, String... additional) throws IOException, JAXBException {
        Map<String, String> header = callContext.getHeader().getFullHeader();
        header.put("contentLength",callContext.getHeader().getContentLength().toString());
        if (additional.length > 0 && additional.length % 2 == 0) {
            for (int i = 0; i < additional.length; i = i + 2) {
                header.put(additional[i], additional[i + 1]);
            }
        }
        header.put("x-amz-date",(new Date()).getTime()+"");
//        header.put("etag",callContext.getHeader().getEtag());
        byte[] bs = SerializationUtil.serializeMap(header);
        LOG.info("meta length :::"+bs.length);
//        Map<String, String> headers = SerializationUtil.deserializeMap(bs);
        Files.createDirectories(meta.getParent());
        try (OutputStream out = Files.newOutputStream(meta)) {
            Marshaller m = jaxbContext.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            m.marshal(new StorageMeta(header), out);
        }
        return bs;
    }


    /**
     * to-do
     * @param callContext
     * @param bucketName
     */
    @Override
    public void deleteBucket(S3CallContext callContext, String bucketName) {
        //1、判断bucket是否存在
        boolean isExistBucket = this.checkBucketExist(bucketName);
        //3、调用删除接口
        try {
            BucketHandler.deleteBucket(bucketName);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public S3Object createObject(S3CallContext callContext, String bucketName, String objectKey) {
        //1、判断链上是否存在此bucket，如果不存在创建，bucketName必须合法
        boolean isBucketExist = this.checkBucketExist(bucketName);
        byte[] metaByte = new byte[0];
        if(!isBucketExist) {
            //如果不存在 则创建bucket
            Map<String,String> header = new HashMap<>();
            String version_status = header.get("version_status");
            if(version_status == null || "".equals(version_status)) {
                header.put("version_status","Enabled");
            }
            byte[] new_byte = SerializationUtil.serializeMap(header);


            try {
                BucketHandler.createBucket(bucketName,new_byte);

            } catch (ServiceException e) {
                LOG.info("ERR:",e);
                e.printStackTrace();
            }
        }

        if ("on".equals(status_sync)) {
            LOG.info("AYNC UPLOAD STARTING...........");
            this.ayncFileUpload(callContext,bucketName,objectKey);
            return null;
        }else {

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
            String tempFileName = UUID.randomUUID().toString();
            //腾讯云备份*****************************



            //腾讯云备份*****************************
            Path meta = metaBucket.resolve(tempFileName + META_XML_EXTENSION);
            Path obj = dataBucket.resolve(tempFileName);

            Long contentLength = callContext.getHeader().getContentLength();
            lock(metaBucket, tempFileName, S3Lock.LockType.write, callContext);
            try (InputStream in = callContext.getContent()) {
                Files.createDirectories(obj.getParent());
                Files.createFile(obj);

                //腾讯云备份*****************************
                OutputStream aes = null;
                Path AESKyeObj=null;
                if("on".equals(cosBackUp)){
                    String aesTmpObj = UUID.randomUUID().toString();
                    AESKyeObj = dataBucket.resolve(aesTmpObj);
                    LOG.info("AESKyeObj====="+AESKyeObj.toString());
                    Files.createFile(AESKyeObj);
                    aes = Files.newOutputStream(AESKyeObj);
                }



                //腾讯云备份*****************************

                DigestInputStream din = new DigestInputStream(in, MessageDigest.getInstance("MD5"));

                try (OutputStream out = Files.newOutputStream(obj)) {

                    //腾讯云备份*****************************
//                    OutputStream
                    //腾讯云备份*****************************
                    long bytesCopied = 0;

                    //腾讯云备份******************
                    try {
                        bytesCopied = CosBackupService.copy(din,out,aes,cosBackUp);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if("on".equals(cosBackUp)){
                        aes.flush();
                        aes.close();
                    }
                    //腾讯云备份******************
//                    bytesCopied = StreamUtils.copy(callContext.getContent(), out);
                    byte[] md5bytes = din.getMessageDigest().digest();
//                    String storageMd5base64 = BaseEncoding.base64().encode(md5bytes);
                    String storageMd5base16 = Encoding.toHex(md5bytes);

                    if (contentLength != null && contentLength != bytesCopied ) {
                        Files.delete(obj);
                        Files.deleteIfExists(meta);
                        throw new InvalidDigestException(objectKey, callContext.getRequestId());
                    }
                    out.close();
                    S3ResponseHeader header = new S3ResponseHeaderImpl();
                    header.setEtag(inQuotes(storageMd5base16));
                    header.setDate(new Date(Files.getLastModifiedTime(obj).toMillis()));
                    callContext.setResponseHeader(header);

                    Files.createDirectories(meta.getParent());
                    metaByte = writeMetaFile(meta, callContext, S3Constants.ETAG, inQuotes(storageMd5base16));
                    Map<String,String> desMap = SerializationUtil.deserializeMap(metaByte);
                    desMap.put("ETag",inQuotes(storageMd5base16));
                    metaByte = SerializationUtil.serializeMap(desMap);
                    //判断文件在链上是否存在，默认不存在
                    boolean isFileExist = false;
                    try {
//                    ObjectId versinoId = new ObjectId(callContext.getParams().getAllParams().get("versionId"));
                        isFileExist = ObjectHandler.isExistObject(bucketName,objectKey,null);
                    } catch (ServiceException e) {
                        e.printStackTrace();
                    }
                    // 如果文件不存在  将文件上传至超级节点
                    String filePath = repoBaseUrl + "/" + bucketName+"/data/"+tempFileName;
                    LOG.info("filePath===="+filePath);
                    UploadObject uploadObject = new UploadObject(filePath);
                    ProgressUtil.putUploadObject(bucketName,objectKey,uploadObject);
                    if(isFileExist == false && contentLength == 0) {
                        try {
                            ObjectId VNU = new ObjectId(defaultVNU);
                            ObjectHandler.createObject(bucketName, objectKey, VNU, metaByte);

                            LOG.info("[ "+objectKey +" ]"+ " uploaded successfully................");

                        } catch (ServiceException e) {
                            LOG.info(objectKey+"  upload failed.111",e);
                            if(Files.exists(AESKyeObj)) {
                                Files.delete(AESKyeObj);
                            }

                            e.printStackTrace();
                        }
                    }else if (isFileExist == false && contentLength > 0) {

                        try {
                            uploadObject.upload();
                            ObjectHandler.createObject(bucketName, objectKey, uploadObject.getVNU(), metaByte);
                            LOG.info("[ "+objectKey +" ]"+ " uploaded successfully................");
                            isFileExist = true;

                            //腾讯云备份*****************************
                            //备份到腾讯云
                            if("on".equals(cosBackUp)) {
                                LOG.info("SYNC UPLOAD,Start backup COS task..................");
                                CosBackupService cosBackupService  = new CosBackupService();
                                String etag = cosBackupService.uploadFile(AESKyeObj.toString(),bucketName,objectKey,cosBucket);
                                LOG.info("etag coss====="+etag);
                            }
                            try {
                                if(Files.exists(AESKyeObj)) {
                                    Files.delete(AESKyeObj);
                                }

                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            LOG.info("SYNC UPLOAD,Complete backup COS task..................");
                            //腾讯云备份*****************************
                        } catch (Exception e) {
                            LOG.info(objectKey + "  upload failed.222",e);
                            if(Files.exists(AESKyeObj)) {
                                Files.delete(AESKyeObj);
                            }
                            e.printStackTrace();
                        }

                    } else {
                        //判断当前bucket是否开启了版本控制，根据版本控制状态决定是否允许上传同名文件
                        Map<String,byte[]> map = null;
                        try {
                            map = BucketHandler.getBucketByName(bucketName);
                        } catch (ServiceException e) {
                            e.printStackTrace();
                        }
                        byte[] bucketMeta = map.get(bucketName);
                        Map<String,String> bucketHeader = new HashMap<>();
                        bucketHeader = SerializationUtil.deserializeMap(bucketMeta);
                        String version_status = bucketHeader.get("version_status");

                        if("Off".equals(version_status) || "OFF".equals(version_status) || version_status==null || "".equals(version_status)) {
                            LOG.error("The file already exists on the chain or has a duplicate file name");
                            throw new InternalErrorException(objectKey, callContext.getRequestId());
                        } else if("Suspended ".equals(version_status) || "SUSPENDED".equals(version_status)) {
                            LOG.error("Currently bucket versioning has been suspended");
                            throw new InternalErrorException(objectKey, callContext.getRequestId());
                        } else if("Enabled".equals(version_status) || "ENABLED".equals(version_status)) {
                            //同名文件生成历史版本
                            try {
                                if(contentLength == 0) {
                                    ObjectHandler.createObject(bucketName, objectKey, new ObjectId(defaultVNU), metaByte);
                                    LOG.info("[ "+objectKey +" ]"+ " uploaded successfully................");
                                } else {
                                    uploadObject.upload();
                                    ObjectHandler.createObject(bucketName, objectKey, uploadObject.getVNU(), metaByte);
                                    LOG.info("[ "+objectKey +" ]"+ " uploaded successfully................");
                                }

                            } catch (ServiceException e) {
                                LOG.info(objectKey + "  upload failed.333",e);
                                if(Files.exists(AESKyeObj)) {
                                    Files.delete(AESKyeObj);
                                }

                                e.printStackTrace();
                            }
                        }
                    }
                }

            } catch (IOException | NoSuchAlgorithmException | JAXBException e) {
                LOG.error("internal error", e);
                e.printStackTrace();
            } catch (InterruptedException e) {
                LOG.error("interrupted thread", e);
                e.printStackTrace();
            } finally {
                try {
                    unlock(metaBucket, tempFileName, callContext);
                    //删除缓存文件
                    LOG.info("Delete ******* CACHE FILE..........."+objectKey);
                    Thread.sleep(60000);
                    ProgressUtil.removeUploadObject(bucketName,objectKey);
                    Files.delete(obj);
                    Files.deleteIfExists(meta);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }


            Map<String,String> header  = SerializationUtil.deserializeMap(metaByte);
            S3Metadata s3Metadata = getMetaMessage(header);
            return new S3ObjectImpl(objectKey,
                    new Date(),
                    bucketName,
                    Long.parseLong(header.get("contentLength")),
                    new S3UserImpl(),
                    s3Metadata,
                    null, s3Metadata.get(S3Constants.CONTENT_TYPE),
                    s3Metadata.get(header.get("ETag")), s3Metadata.get(S3Constants.VERSION_ID), false, true);

        }

    }

    private void ayncFileUpload(S3CallContext callContext, String bucketName, String objectKey) {
        Path syncPath = Paths.get(repoBaseUrl+"/"+bucketName);
        if (!Files.exists(syncPath)) {
            try {
                Files.createDirectories(syncPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String tempFileName = UUID.randomUUID().toString();

        //腾讯云备份*************
        OutputStream aes=null;
        Path aesFilePath = null;
        Path cosPathXML = null;
        if("on".equals(cosBackUp)) {
            String escFileName = UUID.randomUUID().toString();
            aesFilePath = Paths.get(repoBaseUrl+"/"+bucketName+"/"+escFileName);
            cosPathXML = Paths.get(repoBaseUrl + "/" + "cos_xml/" + tempFileName+".xml");
            try {
                aes =  Files.newOutputStream(aesFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //腾讯云备份*************

        Path filePath = Paths.get(repoBaseUrl+"/"+bucketName+"/"+tempFileName+".dat");
        Path filePathXML = Paths.get(repoBaseUrl+"/"+"xml/"+tempFileName+".xml");


        try (InputStream in = callContext.getContent()) {
            DigestInputStream din = new DigestInputStream(in, MessageDigest.getInstance("MD5"));
            OutputStream out = Files.newOutputStream(filePath);
//            long bytesCopied = ayncCopy(din, out,null);
            //腾讯云备份*************
            long bytesCopied = CosBackupService.copy(din,out,aes,cosBackUp);
//            long bytesCopied = ayncCopy(din, out,aes);
            //腾讯云备份*************

            byte[] md5bytes = din.getMessageDigest().digest();
//            String storageMd5base64 = BaseEncoding.base64().encode(md5bytes);
//            String md5 = callContext.getHeader().getContentMD5();
//            if (callContext.getHeader().getContentLength() != null && callContext.getHeader().getContentLength() != bytesCopied
//                    || md5 != null && !md5.equals(storageMd5base64)) {
//                Files.delete(filePath);
//                throw new InvalidDigestException(objectKey, callContext.getRequestId());
//            }

            String storageMd5base16 = Encoding.toHex(md5bytes);

            S3ResponseHeader header = new S3ResponseHeaderImpl();
            String etag = inQuotes(storageMd5base16);
            header.setEtag(inQuotes(storageMd5base16));
            header.setDate(new Date(Files.getLastModifiedTime(filePath).toMillis()));
            callContext.setResponseHeader(header);
            out.close();
            in.close();

            //腾讯云备份*************
            if("on".equals(cosBackUp)){
                aes.close();
            }

            //腾讯云备份*************


            AyncFileMeta fileMeta = new AyncFileMeta();
            fileMeta.setKey(objectKey);
            fileMeta.setPath(filePath.toString());

            //腾讯云备份*************
            if("on".equals(cosBackUp)) {
                fileMeta.setAesPath(aesFilePath.toString());
                LOG.info("cos_bucket========"+cosBucket);
                fileMeta.setCosBucket(cosBucket);
            }

            //腾讯云备份*************


            fileMeta.setBucketname(bucketName);
            byte[] meta = null;
            if("on".equals(cosBackUp)) {
                meta = getMeta(filePath.toString(),filePathXML.toString(),cosPathXML.toString(),etag);
            } else {
                meta = getMeta(filePath.toString(),filePathXML.toString(),null,etag);
            }

            fileMeta.setMeta(meta);
            writeAyncFileMeta(fileMeta,tempFileName,filePathXML);


            AyncUploadSenderPool.putAyncFileMeta(fileMeta);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long ayncCopy(InputStream in, OutputStream out,OutputStream aes) throws Exception {
        long byteCount = 0L;
        byte[] buffer = new byte[131072];
        AESCoder coder = new AESCoder(Cipher.ENCRYPT_MODE);

        int bytesRead;
        while((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);


            //腾讯云备份*************
            if("on".equals(cosBackUp)) {
                byte[] data1 = coder.update(buffer,0,bytesRead);
                aes.write(data1);
            }

            //腾讯云备份*************



            byteCount += (long)bytesRead;
            if(AyncUploadSenderPool.isFull()) {
                Thread.sleep(100);
            }

        }

        //腾讯云备份*************
        if("on".equals(cosBackUp)) {
            byte[] data2 = coder.doFinal();
            aes.write(data2);
            aes.flush();
        }

        //腾讯云备份*************

        out.flush();

        return byteCount;
    }

    //序列化属性 返回byte
    private byte[] getMeta(String path, String xmlMeta,String cosMeta,String etag) {

        File file = new File(path);
        Map<String, String> header = new HashMap<>();
        header.put("contentLength",file.length()+"");
        header.put("x-amz-date",(new Date()).getTime()+"");
        //将完整的xml路径保存在meta中
        header.put("xmlMeta",xmlMeta);
        if(cosMeta != null) {
            header.put("cosMeta",cosMeta);
        }
        header.put("ETag",etag);

        return SerializationUtil.serializeMap(header);
    }

    //异步上传，写meta
    private void writeAyncFileMeta(AyncFileMeta fileMeta, String tempFileName, Path filePathXML) {
        Map<String, String> header = new HashMap<>();
        header.put("objectKey",fileMeta.getKey());
        header.put("uuidKey",tempFileName);
        header.put("bucketName",fileMeta.getBucketname());
        header.put("path",fileMeta.getPath());
        header.put("aesBucket",cosBucket);

        try {
            if(!Files.exists(filePathXML)) {
                Files.createDirectories(filePathXML.getParent());
                Files.createFile(filePathXML);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (OutputStream out = Files.newOutputStream(filePathXML)) {
            Marshaller m = null;
            m = jaxbContext.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            m.marshal(fileMeta, out);
            out.close();
        }catch (IOException | JAXBException e) {
            LOG.info("err::",e);
        }
    }


    //异步上传，读meta
    public AyncFileMeta loadAyncFileMeta(Path meta) {
        try (InputStream in = Files.newInputStream(meta)) {
            AyncFileMeta metaData = (AyncFileMeta) jaxbContext.createUnmarshaller().unmarshal(in);
            return metaData;
        } catch (IOException | JAXBException e) {
            LOG.warn("error reading meta file at " + meta.toString(), e);
        }
        return null;
    }



    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(S3CallContext callContext, String bucketName, String objectKey) {

        String uploadId = UUID.randomUUID().toString();
        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult(bucketName,objectKey,uploadId);
        return result;
    }

    @Override
    public void createUploadPart(S3CallContext callContext, String bucketName, String objectKey) {
        String uploadId = callContext.getParams().getAllParams().get("uploadId");
        Integer partNumber = Integer.valueOf(callContext.getParams().getAllParams().get("partNumber"));
        //1、判断链上是否存在此bucket
        boolean isBucketExist = this.checkBucketExist(bucketName);

        if(!isBucketExist) {
            Map<String,String> header = new HashMap<>();
            String version_status = header.get("version_status");
            if(version_status == null || "".equals(version_status)) {
                header.put("version_status","Enabled");
            }
            byte[] new_byte = SerializationUtil.serializeMap(header);
            try {
                BucketHandler.createBucket(bucketName,new_byte);
            } catch (ServiceException e) {
                e.printStackTrace();
            }
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
        //分片文件
        String tempFileName = UUID.randomUUID().toString();
        Path obj = dataBucket.resolve(tempFileName);
        Path meta = metaBucket.resolve(tempFileName + META_XML_EXTENSION);
        Long contentLength = callContext.getHeader().getContentLength();
        String sha256 = callContext.getHeader().getXamzContentSha256();
        String md5 = callContext.getHeader().getContentMD5();
        try (InputStream in = callContext.getContent()){
            if (!Files.exists(obj)) {
                Files.createDirectories(obj.getParent());
                Files.createFile(obj);
            }

            //文件路径 要创建的文件，把分片数据写到此文件中
            Path file = dataBucket.resolve(obj);

            DigestInputStream din = null;
            try {
                din = new DigestInputStream(in, MessageDigest.getInstance("MD5"));
            } catch (NoSuchAlgorithmException e1) {
                e1.printStackTrace();
            }
            DigestInputStream dinSHA256 = null;
            try {
                dinSHA256 = new DigestInputStream(din, MessageDigest.getInstance("SHA-256"));
            } catch (NoSuchAlgorithmException e1) {
                e1.printStackTrace();
            }
            byte[] md5bytes;
            byte[] sha256bytes;
//            try (OutputStream out = Files.newOutputStream(obj)) {
            FileOutputStream fos = null;
            try {
                fos =  new FileOutputStream(obj.toString());
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            BufferedOutputStream out = new BufferedOutputStream(fos);
                long bytesCopied = 0;
                try {
                    bytesCopied = de.mindconsulting.s3storeboot.util.StreamUtils.copy(dinSHA256,out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                md5bytes = din.getMessageDigest().digest();
                sha256bytes = dinSHA256.getMessageDigest().digest();
                String storageMd5base64 = BaseEncoding.base64().encode(md5bytes);
                String storageMd5base16 = Encoding.toHex(md5bytes);
                String storeSha256Base16 = Encoding.toHex(sha256bytes);
                if (contentLength != null && contentLength != bytesCopied
                        || md5 != null && !md5.equals(storageMd5base64)
                        || sha256!=null && !sha256.equals(storeSha256Base16)) {
                    try {
                        Files.delete(obj);
                        Files.deleteIfExists(meta);
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }

                    throw new InvalidDigestException(objectKey, callContext.getRequestId());
                }

                S3ResponseHeader header = new S3ResponseHeaderImpl();
                String etag = inQuotes(storageMd5base16);
                Date date = new Date(Files.getLastModifiedTime(obj).toMillis());
                header.setEtag(etag);
                header.setDate(date);
                header.setContentLength(0L);
                callContext.setResponseHeader(header);
                Part part = new Part();
                part.setEtag(etag);
                part.setPartNumber(partNumber);
                part.setSize(contentLength);
                part.setLastModified(date);
                part.setTempFilePath(obj.toString());
                part.setFilePath(file.toString());
                MuLtipartUploadCache.insert(uploadId,part);

            } catch (IOException e1) {
            e1.printStackTrace();
        }

    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(S3CallContext callContext, String bucketName, String objectKey) {
        String uploadId = callContext.getParams().getAllParams().get("uploadId");
        List<Part> parts = MuLtipartUploadCache.getParts(uploadId);
//        String filePath = repoBaseUrl+ "/" + bucketName + "/" + "data/" +  UUID.randomUUID().toString();

        String tempFileName = UUID.randomUUID().toString();

        String filePath = repoBaseUrl+ "/" + bucketName + "/" +"data"+"/"+ tempFileName+".dat";
        String filePathXML = repoBaseUrl+ "/" + bucketName + "/"+"data"+"/" + tempFileName+".xml";
        String cosXML = "";
        if("on".equals(status_sync)) {
            filePathXML = repoBaseUrl + "/" + "xml/" + tempFileName + ".xml";
            cosXML = repoBaseUrl + "/" + "cos_xml/" + tempFileName + ".xml";
            Path cosPath = Paths.get(cosXML);
            if(!Files.exists(cosPath) && "on".equals(cosBackUp)) {
                try {
                    Files.createDirectories(cosPath.getParent());
                    Files.createFile(cosPath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //腾讯云备份*************
        String aesFilePath = repoBaseUrl+ "/" + bucketName + "/" +"data"+"/" + tempFileName;
        AESCoder coder = null;
        try {
            coder = new AESCoder(Cipher.ENCRYPT_MODE);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        //腾讯云备份*************


        long size = 0l;
        String etag = null;
        OutputStream bos=null;
        InputStream bis=null;
        OutputStream aes = null;
        try {
            bos=new BufferedOutputStream(new FileOutputStream(filePath));

            //腾讯云备份*************
            if("on".equals(cosBackUp)) {
                aes = new BufferedOutputStream(new FileOutputStream(aesFilePath));
            }

            //腾讯云备份*************

            for (Part part : parts) {
                size += part.getSize();
                File file = new File(part.getTempFilePath());
                bis=new BufferedInputStream(new FileInputStream(file));
                int len=0;
                byte[] bt=new byte[1024];
                while (-1!=(len=bis.read(bt))) {
                    bos.write(bt, 0, len);


                    //腾讯云备份*************
                    if("on".equals(cosBackUp)){
                        byte[] data1 = coder.update(bt,0,bt.length);
                        aes.write(data1);
                    }

                    //腾讯云备份*************
                }
                //腾讯云备份*************
                if("on".equals(cosBackUp)){
                    byte[] data2 = coder.doFinal();
                    aes.write(data2);
                }

                //腾讯云备份*************

                bis.close();
            }
            bos.flush();
            bos.close();
            if("on".equals(cosBackUp)){
                aes.flush();
                aes.close();
            }


            LOG.info("File sharding merged successfully,File size====="+size);
            //                //删除分片文件
            for(Part part : parts) {
                Path tempFile = Paths.get(part.getTempFilePath());
                Files.delete(tempFile);
            }

            // 如果文件不存在  将文件上传至超级节点
//            UploadObject uploadObject = new UploadObject(filePath);
            Path meta = Paths.get(filePathXML);
            Path obj = Paths.get(filePath);
            InputStream in = new FileInputStream(filePath);
            DigestInputStream din = new DigestInputStream(in, MessageDigest.getInstance("MD5"));
            byte[] md5bytes = din.getMessageDigest().digest();
            String storageMd5base16 = Encoding.toHex(md5bytes);

            etag = inQuotes(storageMd5base16);
            //设置Header
            S3ResponseHeader header = new S3ResponseHeaderImpl();
            header.setEtag(etag);
            header.setDate(new Date(Files.getLastModifiedTime(obj).toMillis()));
            callContext.setResponseHeader(header);
            String contentLength = String.valueOf(size);
            byte[] metaByte = writeMetaFile(meta, callContext, S3Constants.ETAG, inQuotes(storageMd5base16));
            Map<String,String> map = SerializationUtil.deserializeMap(metaByte);
            map.put("contentLength",contentLength);
            map.put("content-length",contentLength);
            byte[] newHeaderByte = SerializationUtil.serializeMap(map);
            LOG.info("HERE IS WRITE.............status_aync..."+status_sync);
            LOG.info("on".equals(status_sync));
            //准备在此加异步上传。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。
            if("on".equals(status_sync)) {
                byte[] aync_meta = getMeta(filePath,filePathXML,cosXML,etag);
                AyncFileMeta fileMeta = new AyncFileMeta();
                fileMeta.setKey(objectKey);
                fileMeta.setBucketname(bucketName);
                fileMeta.setMeta(aync_meta);
                fileMeta.setPath(filePath);

                //腾讯云备份***************
                fileMeta.setAesPath(aesFilePath);
                fileMeta.setCosBucket(cosBucket);
                //腾讯云备份
                //将文件信息写入到本地xml中
                writeAyncFileMeta(fileMeta,tempFileName,Paths.get(filePathXML));
                if("on".equals(cosBackUp)){
                    writeAyncFileMeta(fileMeta,tempFileName,Paths.get(cosXML));
                }



                //将上传请求加入到队列
                LOG.info("AYNC QUEuE.........................................");
                AyncUploadSenderPool.putAyncFileMeta(fileMeta);


            }else {
                try {
                    UploadObject uploadObject = new UploadObject(filePath);
                    ProgressUtil.putUploadObject(bucketName,objectKey,uploadObject);
                    uploadObject.upload();
                    ObjectHandler.createObject(bucketName, objectKey, uploadObject.getVNU(), newHeaderByte);

                    LOG.info("[ "+objectKey+" ]" + "uploaded successfully................");

                    //删除缓存文件
                    LOG.info("Delete ******* CACHE FILE...........");
                    in.close();
                    din.close();


                    //备份到腾讯云*****************
                    if("on".equals(cosBackUp)) {
                        CosBackupService cosBackupService = new CosBackupService();
                        String aesEtag = cosBackupService.uploadFile(aesFilePath,bucketName,objectKey,cosBucket);
                        LOG.info("aesEtag:::"+aesEtag);
                    }
                    //备份到腾讯云*****************
                    Files.deleteIfExists(meta);
                    Files.deleteIfExists(obj);

                    //备份到腾讯云*****************
                    Path aesPath = Paths.get(aesFilePath);
                    Files.deleteIfExists(aesPath);
                    LOG.info("BACKUP COMPLETE，etag:::"+etag);
                    //备份到腾讯云*****************
                } catch (Exception e) {
                    LOG.info("File upload failed.444",e);
                    e.printStackTrace();
                }finally {
                    //上传成功或者失败删除进度cache
                    Thread.sleep(60000);
                    ProgressUtil.removeUploadObject(bucketName,objectKey);
                }
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        String location = "http://"+ bucketName + ".s3.amazonaws.com/" + objectKey ;
        result.setBucketName(bucketName);
        result.setLocation(location);
        result.setEtag(etag);
        result.setKey(objectKey);
        return result;
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
            LOG.debug("resource not locked", e);
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

        boolean isSrcBucketExist = this.checkBucketExist(srcBucket);
        if(!isSrcBucketExist) {
            throw new NoSuchBucketException(srcBucket, callContext.getRequestId());
        }
        boolean isDestBucketExist = this.checkBucketExist(destBucket);
        if(!isDestBucketExist) {
            throw new NoSuchBucketException(destBucket, callContext.getRequestId());
        }
        boolean isExistObject = false;
        boolean isDestObject = false;

        try {
            isExistObject = ObjectHandler.isExistObject(srcBucket,destObjectKey,null);
            isDestObject = ObjectHandler.isExistObject(destBucket,destObjectKey,null);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        if(!isExistObject) {
            throw new NoSuchKeyException(srcObjectKey, callContext.getRequestId());
        }

        if(isDestObject) {
            LOG.error("ERR 301:The file already exists on the chain or has a duplicate file name");
            throw new InternalErrorException(destObjectKey, callContext.getRequestId());
        }


        FileMetaMsg fileMetaMsg = null;
        try {
            fileMetaMsg = ObjectHandler.copyObject(srcBucket,destObjectKey,destBucket,destObjectKey);
            LOG.info("COPY OBJECT SUCCESS");
        } catch (ServiceException e) {
            e.printStackTrace();
        }

        Map<String, String> header = SerializationUtil.deserializeMap(fileMetaMsg.getMeta());
        S3Metadata s3Metadata = getMetaMessage(header);
        return new S3ObjectImpl(destObjectKey,
                new Date(),
                destBucket,
                Long.parseLong(header.get("contentLength")),
                new S3UserImpl(),
                s3Metadata,
                null, s3Metadata.get(S3Constants.CONTENT_TYPE),
                s3Metadata.get(S3Constants.ETAG), s3Metadata.get(S3Constants.VERSION_ID), false, true);

    }

    private String[] toArray(S3Metadata srcObjectMeta) {
        List<String> result = new ArrayList<String>();
        ((S3MetadataImpl) srcObjectMeta).forEach((k, v) -> {
            result.add(k);
            result.add(v);
        });
        return result.toArray(new String[0]);
    }

    private boolean checkBucketExist(String bucketName){
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

        LOG.info("head======="+head);

        if(callContext.getParams().getAllParams().containsKey("uploads")) {
            return;
        }
//        boolean isBucketExist = this.checkBucketExist(bucketName);
//        if(!isBucketExist) {
//            throw new NoSuchBucketException(bucketName, callContext.getRequestId());
//        }
        //isObjectExist 为true,表示可以从链上获取到文件，为false则说明当前文件在当前bucket下不存在
        boolean isObjectExist = false;
        ObjectId versionId = null;
            String versionIdd = callContext.getParams().getAllParams().get("versionId");
            if(null == versionIdd || "".equals(versionIdd)) {
                versionIdd = null;
            }else {
                versionId = new ObjectId(versionIdd);
            }

//            isObjectExist = ObjectHandler.isExistObject(bucketName,objectKey,versionId);
//            if(isObjectExist == false) {
////                return;
//                throw new NoSuchKeyException(objectKey, callContext.getRequestId());
//            }

        //防止链上有bucket，本地没有，下载文件之前提前创建，目的是从链上拿到的文件先放至缓存中
//        Path dataBucket = Paths.get(repoBaseUrl, bucketName, DATA_FOLDER);
//        Path metaBucket = Paths.get(repoBaseUrl, bucketName, META_FOLDER);
//        if (!Files.exists(dataBucket)){
//            try {
//                Files.createDirectories(dataBucket);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        if(!Files.exists((metaBucket))) {
//            try {
//                Files.createDirectories(metaBucket);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        DownloadObject obj= null;
        try {
            obj = new DownloadObject(bucketName, objectKey,versionId);
        } catch (ServiceException e) {
            if(obj != null) {
                LOG.info("ERR:",e);
            }
        }
        InputStream is= null;
        //如果range不为空，则表示范围下载 range格式 例：range: "bytes=0-2000"
        String range = callContext.getHeader().getRange();
        if(obj != null) {
            LOG.info("File size = "+obj.getLength());
            if(obj.getLength() < allowMaxSize) {
                range = null;
            }
        }

        if(range == null && obj != null) {
//            if(head == false) {
//                is = obj.load();
//                callContext.setContent(is);
////                LOG.info("Download [" + objectKey + "] is Success..." );
//            }
            is = obj.load();
        } else {
            String newRange = range.replace("bytes=","");
            LOG.info("range======"+range);
            long start = Long.parseLong(newRange.substring(0,newRange.indexOf("-")));
            long end = Long.parseLong(newRange.substring(newRange.indexOf("-")+1));
            LOG.info("start======"+start + "   /end======"+end);
            is = obj.load(start,end);
        }
        try {
            S3ResponseHeader header = new S3ResponseHeaderImpl();
            header.setContentLength(obj.getLength());
            header.setContentType("application/octetstream");
            callContext.setResponseHeader(header);
            callContext.setContent(is);
            LOG.info("Download [" + objectKey + "] is Success..." );
//            if (!head)
//                callContext.setContent(is);
        } catch (Exception e) {
            LOG.error("internal error", e);
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
            LOG.warn("error reading meta file at " + meta.toString(), e);
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
//        Integer maxKeys = 100;
        String marker = callContext.getParams().getMarker();
        String prefix = callContext.getParams().getPrefix() != null ? callContext.getParams().getPrefix() : "";
        String delimiter = callContext.getParams().getDelimiter();
        boolean isVersion = callContext.getParams().listVersions();
        String nextVersionId = callContext.getParams().getVersionIdMarker();
        ObjectId nextId = null;
        if(nextVersionId != null) {
            nextId = new ObjectId(nextVersionId);
        } else {
            nextId = null;
        }
        String fileName = null;
        if(marker != null) {
            fileName = marker;
        }

        List<S3Object> s3Objects = new ArrayList<>();
        try {
            List<FileMetaMsg> fileMetaMsgs = ObjectHandler.listBucket(bucketName,fileName,prefix,isVersion,nextId,maxKeys);
            if(fileMetaMsgs.size() > 0) {
                for(FileMetaMsg fileMetaMsg : fileMetaMsgs) {
                    byte[] meta = fileMetaMsg.getMeta();
                    Map<String, String> header = SerializationUtil.deserializeMap(meta);
                    S3Metadata s3Metadata = getMetaMessage(header);
                    String lastModified = header.get("x-amz-date");
                    if(lastModified == null) {
                        lastModified = header.get("date");
                    }
                    Date date = null;
                    try{
                        long ss =Long.parseLong(lastModified);
                        date = new Date(ss);
                    }catch (Exception e) {
                        date = new Date(lastModified);
                    }
                    String etag = header.get("ETag");
                    if(etag ==null) {
                        header.put("ETag",S3Constants.ETAG);
                    }
                    long size = Long.parseLong(header.get("contentLength"));
                    s3Objects.add(new S3ObjectImpl(
                            fileMetaMsg.getFileName(),
                            date,
                            bucketName,
                            size,
                            new S3UserImpl(),
                            s3Metadata,
                            null,
                            s3Metadata.get(S3Constants.CONTENT_TYPE),
                            s3Metadata.get(header.get("ETag")),
                            s3Metadata.get(S3Constants.VERSION_ID),
                            false,
                            true));
                }
            }
            if(fileMetaMsgs.size()<maxKeys) {
                marker = null;
                nextVersionId = null;
            } else {

                marker = fileMetaMsgs.get(fileMetaMsgs.size()-1).getFileName();
                nextVersionId = fileMetaMsgs.get(fileMetaMsgs.size()-1).getVersionId().toString();
            }

        } catch (ServiceException e) {
            e.printStackTrace();
        }
        if(delimiter == null) {
            return new S3ListBucketResultImpl(s3Objects, null, marker!=null, bucketName, marker, nextVersionId);
        } else {
            if(!delimiter.equals("/"))
                throw new InvalidTokenException(delimiter, callContext.getRequestId());
            Set<String> prefixes = new HashSet<>();
            for(S3Object obj : s3Objects) {
                String commonPrefix = DelimiterUtil.getCommonPrefix(obj.getKey(), prefix, "/");
                if(commonPrefix != null) {
                    prefixes.add(commonPrefix);
                } else {
//                        s3Objects.add(obj);
                }
            }
            List<String> prefList = new ArrayList<>(prefixes);
            return new S3ListBucketResultImpl(s3Objects, prefixes.isEmpty()?null:prefList, marker!=null , bucketName, null, nextVersionId);
        }
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
        String versionIdd = callContext.getParams().getAllParams().get("versionId");
        ObjectId versionId = new ObjectId(defaultVNU);
        if(null == versionIdd || "".equals(versionIdd)) {
            versionId = null;
        } else {
            versionId = new ObjectId(versionIdd);
        }

        boolean isExistBucket = this.checkBucketExist(bucketName);
        if(isExistBucket == false) {
            throw new NoSuchBucketException(bucketName, callContext.getRequestId());
        }
        boolean isExistObject = false;
        try {
            isExistObject = ObjectHandler.isExistObject(bucketName,objectKey,versionId);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        if(!isExistObject) {
            throw new NoSuchKeyException(objectKey, callContext.getRequestId());
        }
        try {
            ObjectHandler.deleteObject(bucketName,objectKey,versionId);
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
        LOG.info("getuser()    accessKey======="+accessKey);
        return loadUser(callContext,accessKey);
    }

    private S3User loadUser(S3CallContext callContext, String accessKey) {
        LOG.info("loadUser()    accessKey======="+accessKey);
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

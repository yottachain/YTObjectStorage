package de.mindconsulting.s3storeboot;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.ytfs.common.ServiceException;
import de.mindconsulting.s3storeboot.util.S3ClientUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class S3StoreBootApplicationTests {

//	final AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();

	@Test
	public void contextLoads() {
	}
	@Before
	public void setUp() {

	}
	public AmazonS3Client getClient() {
		S3ClientUtil s3Clientutil = new S3ClientUtil();
		AmazonS3Client s3Client = s3Clientutil.getClient();
		return s3Client;
	}

	@Test
	public void testGetObject() throws IOException {
		AmazonS3Client client = this.getClient();
		File file = new File("E://Engkust.docx");

		InputStream input = new FileInputStream(file);
		Map<String,String> map = new HashMap<>();
		map.put("owner","test001");
		map.put("hello","world");
		map.put("username","penghuaisong");
		ObjectMetadata meta = new ObjectMetadata();

		meta.setLastModified(new Date());
		meta.setContentLength(file.length());
		meta.setUserMetadata(map);
		client.putObject("x-test", file.getName(),input, meta);
		System.out.println("over");

	}
	@Test
	public void testSubStr(){
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		sdf1.setTimeZone(TimeZone.getTimeZone("UTC"));
		String str = sdf1.format(new Date());
		System.out.println("tz================"+str);
	}
	@Test
	public void testString() {
		AmazonS3Client client = this.getClient();
		ListVersionsRequest request = new ListVersionsRequest();
		request.withBucketName("wqqqq").withMaxResults(100).withVersionIdMarker(null);
		VersionListing objects = client.listVersions(request);
		System.out.println("======================");
//		ObjectListing objectListing = client.listObjects("versions");
//		String nextMarker = null;
//		String nextVersionIdMarker = null;
//		do{
//			ListVersionsRequest request = new ListVersionsRequest();
//			request.withBucketName("versions").withMaxResults(100).withVersionIdMarker(nextVersionIdMarker);
//			VersionListing objects = client.listVersions(request);
//			for (S3VersionSummary s3VersionSummary : objects.getVersionSummaries()) {
//				System.out.println(s3VersionSummary.getKey());
//				System.out.println(s3VersionSummary.getVersionId());
//			}
//		} while (objectListing.isTruncated());
//
//
//		VersionListing listing = client.listVersions(request);
//		System.out.println("list size ====" + listing);
	}
	@Test
	public void testVersionObject() {
		AmazonS3Client client = this.getClient();
		BucketVersioningConfiguration configuration = new BucketVersioningConfiguration();
		configuration.setStatus(BucketVersioningConfiguration.ENABLED);
		SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest("new-version",configuration);
		client.setBucketVersioningConfiguration(request);
		System.out.println("开启历史版本成功");
		configuration = client.getBucketVersioningConfiguration("new-version");
		System.out.println("开启历史版本成功");


	}
	@Test
	public void testSerialization(){
//		Map<String,String> map = new HashMap<>();
//		map.put("test1","1234");
//		map.put("test2","3344");
//		map.put("test3","dsssee");
//		map.put("test4","对哦额饿哦额");
//		byte[] bs = SerializationUtil.serializeMap(map);
//
//		Map<String,String> newMap = SerializationUtil.deserializeMap(bs);
//
//		System.out.println(newMap.size());
		AmazonS3Client client = this.getClient();
//		client.listVersions();



	}

	@Test
	public void testGetRangeObject() {
		AmazonS3Client client = new S3ClientUtil().getClient();

		GetObjectRequest request = new GetObjectRequest("ab1","apache-tomcat-7.0.86-windows-x64.zip");
		request.setRange(1000,2000);
		S3Object s3Object = client.getObject(request);
		InputStream in = s3Object.getObjectContent();
		byte[] buf = new byte[4096];


	}



	@Test
	public void test() throws FileNotFoundException {
		AmazonS3Client client = new S3ClientUtil().getClient();
		File file = new File("E://ubuntu-18.04-desktop-amd64.iso");
        InputStream input = new FileInputStream(file);
		String bucketName = "test2";
		ObjectMetadata meta = new ObjectMetadata();
//		meta.setContentLength(2*1024*1024);
		InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName,file.getName(),meta);
		InitiateMultipartUploadResult result = client.initiateMultipartUpload(initiateMultipartUploadRequest);
		String uploadId = result.getUploadId();
		System.out.println("uploadId ====="+uploadId);

        List<PartETag> partETags = new ArrayList<PartETag>();
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.setKey(file.getName());
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(input);
        uploadPartRequest.setPartSize(2*1024*1024);
        uploadPartRequest.setPartNumber(2);

        UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);

        partETags.add(uploadPartResult.getPartETag());

        Collections.sort(partETags, new Comparator<PartETag>() {
                public int compare(PartETag p1, PartETag p2) {
                    return p1.getPartNumber() - p2.getPartNumber();
                }
        });
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName, file.getName(), uploadId, partETags);
        //成功完成分片上传后，服务端会返回该对象的信息
        CompleteMultipartUploadResult over = client.completeMultipartUpload(completeMultipartUploadRequest);

	}


//	private static void writeToLocal(String destination, InputStream input,String fileName)
//			throws IOException {
//		int index;
//		byte[] bytes = new byte[1024];
//		FileOutputStream downloadFile = new FileOutputStream(destination+"/"+fileName);
//		while ((index = input.read(bytes)) != -1) {
//			downloadFile.write(bytes, 0, index);
//			downloadFile.flush();
//		}
//		downloadFile.close();
//		input.close();
//	}


    @Test
    public void testListBuckets() throws ServiceException {

////		String[] objects = BucketHandler.listObjectByBucket("test");
//		String[] buckets =  BucketHandler.listBucket();
//
//
//		//从区块链超级节点中获取bucket集合
//
//
//        for(int i=0;i<buckets.length;i++) {
//        	System.out.println("bucket name: " + buckets[i]);
//		}
		AmazonS3Client client = getClient();
		ObjectListing objectListing = client.listObjects("new-bucket-f26f28e0");
		System.out.println("=========================");

    }

	@Test
	public void testPutBigObjectAndVerifyGet() throws IOException {
		final long TEST_LENGTH = 1024 * 1024 * 1024 * 6L; // 6 GB

		AmazonS3Client client = getClient();
		Bucket b = client.createBucket(UUID.randomUUID().toString());
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(TEST_LENGTH);

		class BigInput extends InputStream {
			final long l;
			long counter = 0L;

			BigInput(long length) {
				l = length;
			}

			@Override
			public int read() throws IOException {
				return counter++ >= l ? -1 : 'M';
			}
		}
		client.putObject(b.getName(), "test.txt", new BigInput(TEST_LENGTH), meta);

		try (S3ObjectInputStream content = client.getObject(b.getName(), "test.txt").getObjectContent()) {
			long readCounter = 0L;
			int byteRead = 0;

			while ((byteRead = content.read()) != -1) {
				assertEquals('M', byteRead);
				readCounter++;
			}
			assertEquals(TEST_LENGTH, readCounter);
		} catch (AmazonClientException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testCopyObject() throws IOException {
		AmazonS3Client client = getClient();
		Bucket b = client.createBucket(UUID.randomUUID().toString());
		ObjectMetadata meta = new ObjectMetadata();
		client.putObject(b.getName(), "test.txt", new ByteArrayInputStream("test".getBytes()), meta);
		client.copyObject(b.getName(), "test.txt", b.getName(), "test2.txt");

	}

}

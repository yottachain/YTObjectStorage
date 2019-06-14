package de.mindconsulting.s3storeboot;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.ytfs.common.ServiceException;
import de.mc.ladon.s3server.common.StreamUtils;
import de.mindconsulting.s3storeboot.util.S3Clientutil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
		S3Clientutil s3Clientutil = new S3Clientutil();
		AmazonS3Client s3Client = s3Clientutil.getClient();
		return s3Client;
	}

	@Test
	public void testGetObject() throws IOException {
		AmazonS3Client client = this.getClient();
		File file = new File("E://test004.txt");

		InputStream input = new FileInputStream(file);
		Map<String,String> map = new HashMap<>();
		map.put("owner","test001");
		map.put("hello","world");
		map.put("username","penghuaisong");
		ObjectMetadata meta = new ObjectMetadata();

		meta.setLastModified(new Date());
		meta.setContentLength(file.length());
		meta.setUserMetadata(map);
//		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.getName(), file.getName(),input, meta);
//		putObjectRequest.setFile(file);
		client.putObject("test001", file.getName(),input, meta);
		System.out.println("over");

	}



	@Test
	public void test() {
		AmazonS3Client client = new S3Clientutil().getClient();
		BucketVersioningConfiguration versioningConfiguration = new BucketVersioningConfiguration();
//		//启用版本控制
		versioningConfiguration.setStatus(BucketVersioningConfiguration.ENABLED);
		SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest("new-bucket-97b2f2d9",versioningConfiguration);
		client.setBucketVersioningConfiguration(request);

		BucketVersioningConfiguration configuration = client.getBucketVersioningConfiguration("new-bucket-97b2f2d9");
		VersionListing versionListing = client.listVersions("new-bucket-97b2f2d9","");
		System.out.println("............................................");
//		client.deleteVersion("new-bucket-97b2f2d9","test03.txt","5cfdf5d851f96b0e06ffa91c");
//		client.putObject("test-huaisong","directory04/", new ByteArrayInputStream(new byte[0]), null);
//		PutObjectResult result = client.putObject("test-huaisong","directory04/", new ByteArrayInputStream(new byte[0]), null);
		//		File file = new File("E://凌子杰.txt");
//		client.putObject("test-huaisong","/test111",file);
//		S3Object s3Object = client.getObject("test-huaisong","directory03");
//		InputStream is=s3Object.getObjectContent();
//		ByteArrayOutputStream out=new java.io.ByteArrayOutputStream();
//		try {
//			long bytesCopied = StreamUtils.copy(is, out);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		String ss=new String(out.toByteArray());
//		System.out.println("====================================="+ss);

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

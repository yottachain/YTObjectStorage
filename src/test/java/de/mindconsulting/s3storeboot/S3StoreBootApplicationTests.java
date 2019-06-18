package de.mindconsulting.s3storeboot;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
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
	public void test() throws FileNotFoundException {
		AmazonS3Client client = new S3Clientutil().getClient();
		File file = new File("E://ubuntu-18.04-desktop-amd64.iso");
		String bucketName = "test2";
		ObjectMetadata meta = new ObjectMetadata();
//		meta.setContentLength(2*1024*1024);
		InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName,file.getName(),meta);
		InitiateMultipartUploadResult result = client.initiateMultipartUpload(initiateMultipartUploadRequest);
		String uploadId = result.getUploadId();
		System.out.println("uploadId ====="+uploadId);

//		File file = new File("E://ubuntu-18.04-desktop-amd64.iso");
//
//		InputStream input = new FileInputStream(file);
//		TransferManager tm = new TransferManager(client);
//		ObjectMetadata meta = new ObjectMetadata();
//		meta.setContentLength(2*1024*1024);
//		Upload upload = tm.upload("test2",file.getName(),input,meta);
//		try {
//			// 等待上传全部完成。     
//			upload.waitForCompletion();
//			System.out.println("Upload complete.");
//		}catch(AmazonClientException amazonClientException) {
//			System.out.println("Unable to upload file, upload was aborted.");
//			amazonClientException.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		tm.shutdownNow();
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

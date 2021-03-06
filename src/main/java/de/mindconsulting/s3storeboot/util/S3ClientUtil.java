package de.mindconsulting.s3storeboot.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.stereotype.Component;

@ImportAutoConfiguration
@Component
public class S3ClientUtil {

    @Value("${s3server.baseUrl}")
   private String repoBaseUrl;

    public AmazonS3Client getClient() {
        AWSCredentials credentials = new BasicAWSCredentials("SYSTEM", "SYSTEM");
        AmazonS3Client newClient = new AmazonS3Client(credentials,
                new ClientConfiguration());
        newClient.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        newClient.setEndpoint("http://localhost:8083");
        return newClient;
    }

}

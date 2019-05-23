package de.mindconsulting.s3storeboot;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

@Component
@SpringBootApplication
@EnableAutoConfiguration(exclude={MongoAutoConfiguration.class})
@ComponentScan(basePackages = {"com.s3.user.controller"})
@ComponentScan(basePackages = {"de.*"})
public class YTObjectStorageApplication {

	private static Logger logger = Logger.getLogger(SpringApplication.class);

	public static void main(String[] args) {

		System.out.println("............................");
		System.out.println("............................");
		System.out.println("............................");
		System.out.println("............................");
		System.out.println("............................");

		SpringApplication.run(YTObjectStorageApplication.class, args);
		System.out.println("............Service..started..............");
	}

}

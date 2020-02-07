package de.mindconsulting.s3storeboot;

import org.apache.catalina.connector.Connector;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

@Component
@SpringBootApplication
@ComponentScan(basePackages = {"com.s3.user.controller"})
@ComponentScan(basePackages = {"de.*"})
public class YTObjectStorageApplication implements WrapperListener{

    private static Logger logger = Logger.getLogger(SpringApplication.class);
    public static void main(String[] args) {
        System.out.println("............................");
        System.out.println("............................");
        System.out.println("............................");
        System.out.println("............................");
        System.out.println("............................");
        WrapperManager.start(new YTObjectStorageApplication(), args);
        System.out.println("............Service..started..............");
    }

    ConfigurableApplicationContext context=null;
    @Override
    public Integer start(String[] strings) {
        context= SpringApplication.run(YTObjectStorageApplication.class, strings);
        return null;
    }

    @Override
    public int stop(int exitCode) {
        SpringApplication.exit(context);
        return exitCode;
    }

    @Override
    public void controlEvent(int event) {
        if (WrapperManager.isControlledByNativeWrapper() == false) {
            if (event == WrapperManager.WRAPPER_CTRL_C_EVENT
                    || event == WrapperManager.WRAPPER_CTRL_CLOSE_EVENT
                    || event == WrapperManager.WRAPPER_CTRL_SHUTDOWN_EVENT) {
                WrapperManager.stop(0);
            }
        }
    }

    //配置http支持
    @Bean
    public ServletWebServerFactory servletContainer() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.addAdditionalTomcatConnectors(createStandardConnector()); // 添加http
        return tomcat;
    }

    private Connector createStandardConnector() {
        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        connector.setPort(8083);
        return connector;
    }
}
//package de.mindconsulting.s3storeboot;
//
//import org.apache.log4j.Logger;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.stereotype.Component;
//
//@Component
//@SpringBootApplication
//@EnableAutoConfiguration(exclude={MongoAutoConfiguration.class})
//@ComponentScan(basePackages = {"com.s3.user.controller"})
//@ComponentScan(basePackages = {"de.*"})
//public class YTObjectStorageApplication {
//
//    private static Logger logger = Logger.getLogger(SpringApplication.class);
//
//    public static void main(String[] args) {
//
//        System.out.println("............................");
//        System.out.println("............................");
//        System.out.println("............................");
//        System.out.println("............................");
//        System.out.println("............................");
//
//        SpringApplication.run(YTObjectStorageApplication.class, args);
//        System.out.println("............Service..started..............");
//    }
//
//}

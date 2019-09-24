package de.mindconsulting.s3storeboot;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
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
}
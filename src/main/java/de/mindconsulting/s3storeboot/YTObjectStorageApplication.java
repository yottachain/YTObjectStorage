package de.mindconsulting.s3storeboot;

import com.ytfs.client.ClientInitor;
import org.apache.catalina.connector.Connector;
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

    public static void main(String[] args) {
        WrapperManager.start(new YTObjectStorageApplication(), args);
    }

    ConfigurableApplicationContext context=null;
    @Override
    public Integer start(String[] strings) {
        context= SpringApplication.run(YTObjectStorageApplication.class, strings);
        return null;
    }

    @Override
    public int stop(int exitCode) {
        ClientInitor.stop();
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

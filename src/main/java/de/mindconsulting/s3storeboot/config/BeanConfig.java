package de.mindconsulting.s3storeboot.config;

import com.ytfs.client.ClientInitor;
import com.ytfs.client.Configurator;
import de.mc.ladon.s3server.logging.LoggingRepository;
import de.mc.ladon.s3server.logging.PerformanceLoggingFilter;
import de.mc.ladon.s3server.repository.api.S3Repository;
import de.mc.ladon.s3server.servlet.S3Servlet;
import de.mindconsulting.s3storeboot.repository.impl.RepositoryImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Configuration
@Component
public class BeanConfig {

    @Value("${s3server.fsrepo.root}")
    String fsRepoRoot;
    @Value("${server.port}")
    String port;
    @Value("${s3server.superNodeID}")
    String superNodeID;
    @Value("${s3server.superNodeAddr1}")
    String superNodeAddr1;
    @Value("${s3server.userID}")
    String userID;
    @Value("${s3server.KUSp}")
    String KUSp;
    @Value("${s3server.superNodeNum}")
    String superNodeNum;
    @Value("${s3server.port}")
    String s3port;
    @Value("${s3server.s3store.accessKey}")
    String accessKey;
    @Value("${s3server.allowMaxSize}")
    int allowMaxSize;
    @Value("${s3server.url}")
    public String url;
    @Value("${s3server.superNodeKey}")
    String superNodeKey;
    @Value("${s3server.username}")
    String username;
    @Value("${s3server.contractAccount}")
    String contractAccount;

    @ConditionalOnMissingBean
    @Bean
    S3Repository s3Repository() {
        return new RepositoryImpl(fsRepoRoot,accessKey,allowMaxSize);
    }

    @Bean
    ServletRegistrationBean s3Registration(S3ServletConfiguration config, S3Repository repository) throws IOException {
        init();
        ServletRegistrationBean bean = new ServletRegistrationBean();
        bean.setName("s3servlet");
        bean.setAsyncSupported(true);
        bean.addUrlMappings(config.getBaseUrl() + "/*");
        S3Servlet s3Servlet = new S3Servlet(config.getThreadPoolSize());
        if (config.isLoggingEnabled()) {
            s3Servlet.setRepository(new LoggingRepository(repository));
        } else {
            s3Servlet.setRepository(repository);
        }
        s3Servlet.setSecurityEnabled(config.isSecurityEnabled());
        bean.setServlet(s3Servlet);
        return bean;
    }


    private  void init() throws IOException {

        List<String> list = Arrays.asList(superNodeAddr1.split(","));
        Configurator cfg = new Configurator();
        cfg.setKUSp(KUSp);
        cfg.setSuperNodeAddrs(list);
        cfg.setSuperNodeID(superNodeID);
        cfg.setSuperNodeNum(Integer.parseInt(superNodeNum));
        cfg.setTmpFilePath(fsRepoRoot);
        cfg.setContractAccount(contractAccount);
        cfg.setUsername(username);
        ClientInitor.init(cfg);
    }

    @Bean
    @ConditionalOnProperty(value = "s3server.loggingEnabled" , havingValue = "true")
    FilterRegistrationBean filterRegistrationBean() {
        FilterRegistrationBean filterBean = new FilterRegistrationBean();
        filterBean.setFilter(new PerformanceLoggingFilter());
        filterBean.addServletNames("s3servlet");
        filterBean.setAsyncSupported(true);
        return filterBean;
    }


}

package de.mindconsulting.s3storeboot.config;

import com.s3.user.controller.sync.task.AyncUploadSenderPool;
import com.ytfs.client.ClientInitor;
import com.ytfs.client.Configurator;
import com.ytfs.client.v2.YTClient;
import com.ytfs.client.v2.YTClientMgr;
import com.ytfs.common.codec.AESCoder;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.conf.UserConfig;
import de.mc.ladon.s3server.logging.LoggingRepository;
import de.mc.ladon.s3server.logging.PerformanceLoggingFilter;
import de.mc.ladon.s3server.repository.api.S3Repository;
import de.mc.ladon.s3server.servlet.S3Servlet;
import de.mindconsulting.s3storeboot.repository.impl.RepositoryImpl;
import de.mindconsulting.s3storeboot.util.AESUtil;
import de.mindconsulting.s3storeboot.util.PropertiesUtil;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Configuration
@Component
public class BeanConfig {

    @Value("${s3server.fsrepo.root}")
    String fsRepoRoot;
    @Value("${server.port}")
    String port;
    @Value("${s3server.userID}")
    String userID;

    @Value("${s3server.superNodeNum}")
    String superNodeNum;
    @Value("${s3server.port}")
    String s3port;
//    @Value("${s3server.s3store.accessKey}")
//    String accessKey;
    @Value("${s3server.allowMaxSize}")
    int allowMaxSize;
    @Value("${s3server.url}")
    public String url;
    @Value("${s3server.uploadShardThreadNum}")
    int uploadShardThreadNum;
    @Value("${s3server.downloadThread}")
    int downloadThread ;
    @Value("${s3server.uploadBlockThreadNum}")
    int uploadBlockThreadNum;

    @Value("${s3server.PNN}")
    int PNN;

    @Value("${s3server.PTR}")
    int PTR;

    @Value("${s3server.RETRYTIMES}")
    int RETRYTIMES;
    @Value("${s3server.zipkinServer}")
    String zipkinServer;

    @Value("${s3server.SYNC}")
    String status_sync;
    @Value("${s3server.SYNC_BUCKET}")
    String syncBucketName;
    @Value("${s3server.queueSize}")
    int queueSize;
    @Value("${s3server.syncCount}")
    int syncCount;
    @Value("${s3server.dirctory}")
    String dirctory;
    @Value("${s3server.cosBucket}")
    String cosBucket;
    @Value("${s3server.cosBackUp}")
    String cosBackUp;
    @Value("${s3server.uploadFileMaxMemory}")
    String uploadFileMaxMemory;
    @Value("${s3server.isOpenUsers}")
    String isOpenUsers;

    private static final Logger LOG = Logger.getLogger(BeanConfig.class);

    @ConditionalOnMissingBean
    @Bean
    S3Repository s3Repository() {

        return new RepositoryImpl(fsRepoRoot,allowMaxSize,status_sync,syncBucketName,syncCount,cosBucket,cosBackUp,isOpenUsers);
    }

    @Bean
    ServletRegistrationBean s3Registration(S3ServletConfiguration config, S3Repository repository) throws IOException {
        if(isOpenUsers.equals("true")) {
            Configurator cfg=new Configurator();
            //矿机列表长度(328-1000)
            cfg.setPNN(PNN);
            //每N分钟更新一次矿机列表(2-10分钟)
            cfg.setPTR(PTR);
            //上传文件时最大分片并发数(50-3000)
            cfg.setUploadShardThreadNum(uploadShardThreadNum);
            //上传文件时最大块并发数(3-500)
            cfg.setUploadBlockThreadNum(uploadBlockThreadNum);
            //上传文件时没文件最大占用5M内存(3-20)
            cfg. setUploadFileMaxMemory(uploadFileMaxMemory);
            //下载文件时最大分片并发数(50-500)
            cfg. setDownloadThread (downloadThread);
            YTClientMgr.init(cfg);
        }else {
            String cert_path = dirctory + "/"+"yts3.conf";
            LOG.info("cert_path===="+cert_path);

            String cert = readCert(cert_path);

            if(!"".equals(cert)) {
                JSONObject jsonStr = JSONObject.fromObject(cert);

                String KUSp = jsonStr.getString("privateKey");
                String username = jsonStr.getString("username");
                init(KUSp,username);
            }
        }
        AyncUploadSenderPool.init(fsRepoRoot,queueSize,syncCount,cosBackUp);
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

    private String readCert(String propertiesPath) {
//        String cert_path = System.getProperty("cert", "conf/cert");
        String cert_path = dirctory +"/"+ "cert";
        Path path = Paths.get(cert_path);
        if (!Files.exists(path)) {
            return "";
        } else {
            PropertiesUtil p = new PropertiesUtil(propertiesPath);
            String SHA256_KEY = p.readProperty("SHA256_KEY");
            UserConfig.AESKey = KeyStoreCoder.generateUserKey(SHA256_KEY.getBytes());
            AESCoder coder = null;
            try {
                coder = new AESCoder(Cipher.DECRYPT_MODE);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (NoSuchPaddingException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            }

            byte[] data1 = new byte[0];
            try {
                data1 = AESUtil.getBytesFromFile(new File(cert_path));
            } catch (Exception e) {
                e.printStackTrace();
            }
            byte[] data2 = new byte[0];
            try {
                data2 = coder.doFinal(data1);
            } catch (IllegalBlockSizeException e) {
                e.printStackTrace();
            } catch (BadPaddingException e) {
                e.printStackTrace();
            }

            return new String(data2);
        }
    }

    private  void init(String private_key,String user_name) throws IOException {

        Configurator cfg = new Configurator();

//        cfg.setTmpFilePath("");
        cfg.setKUSp(private_key);
        cfg.setUsername(user_name);
        cfg.setUploadShardThreadNum(uploadShardThreadNum);
        cfg.setDownloadThread(downloadThread);
        cfg.setUploadBlockThreadNum(uploadBlockThreadNum);
        cfg.setUploadFileMaxMemory(uploadFileMaxMemory);
        cfg.setPNN(PNN);
        cfg.setPTR(PTR);
        cfg.setRETRYTIMES(RETRYTIMES);
        cfg.setZipkinServer(zipkinServer);
        ClientInitor.init(cfg);
    }
//    @Bean
//    public CorsConfiguration buildConfig() {
//        LOG.info("cros***********************************");
//
//        CorsConfiguration corsConfiguration = new CorsConfiguration();
//        corsConfiguration.addAllowedOrigin("*");
//        corsConfiguration.addAllowedHeader("*");
//        corsConfiguration.addAllowedMethod("*");
//        corsConfiguration.setAllowCredentials(true);
//        return corsConfiguration;
//    }

    //springboot 2.0以上的方式
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedHeaders("Content-Type","X-Requested-With","accept,Origin","Access-Control-Request-Method","Access-Control-Request-Headers","token")
                .allowedMethods("*")
                .allowedOrigins("*")
                .allowCredentials(true);
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

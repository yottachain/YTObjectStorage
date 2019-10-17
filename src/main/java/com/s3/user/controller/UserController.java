package com.s3.user.controller;

import com.ytfs.client.LocalInterface;
import com.ytfs.common.ServiceException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping(value="/user")
public class UserController {

    private static final Logger LOG = Logger.getLogger(UserController.class);

    @Value("${s3server.SYNC_DIR}")
    String SYNC_DIR;
    @Value("${s3server.SYNC_BUCKET}")
    String syncBucketName;

    @RequestMapping(value = "/getUserStat",method = RequestMethod.GET)
    @ResponseBody
    public String getUserStat(HttpServletRequest request, HttpServletResponse response) {
        String json = null;
        try {
            response.setHeader("Access-Control-Allow-Origin","*");
            json = LocalInterface.getUserStat();
            System.out.println("userstat==================="+json);
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        return json;
    }

    @RequestMapping(value = "/getPrivateKey",method = RequestMethod.GET)
    @ResponseBody
    public String getPrivateKey(HttpServletRequest request, HttpServletResponse response) {
        String getPrivateKey = null;
        response.setHeader("Access-Control-Allow-Origin","*");
        getPrivateKey = LocalInterface.getPrivateKey();
        System.out.println("getPrivateKey==========="+getPrivateKey);
        return getPrivateKey;
    }

    @RequestMapping(value = "/sync",method = RequestMethod.GET)
    @ResponseBody
    public String getUnFinished() {


        String[] objectList = new File(SYNC_DIR+"/"+syncBucketName).list();

        String status = this.syncStatus(objectList);
        return status;
    }

    public String syncStatus(String[] objectList) {
        try {
            Thread.currentThread().sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String[] tmpFileNameList = objectList;
        List<String> ls = Arrays.asList(tmpFileNameList);
        List tmpList = new ArrayList(ls);
        if(tmpFileNameList.length == 0) {
            LOG.info("success");
        }

        if(objectList.length >0) {
            for(int i=0;i<objectList.length;i++) {
                Path filePath = Paths.get(SYNC_DIR+"/"+syncBucketName+"/"+objectList[i]);
                if(!Files.exists(filePath)) {
                    tmpList.remove(objectList[i]);
                }
            }
            tmpFileNameList = new String[tmpList.size()];
            tmpList.toArray(tmpFileNameList);
            System.out.println("ssssss======="+tmpFileNameList.length);



            if(tmpFileNameList.length>0) {
                return this.syncStatus(tmpFileNameList);
            } else {
                return "SUCCESS";
            }

        } else {
            return "SUCCESS";
        }
    }

}

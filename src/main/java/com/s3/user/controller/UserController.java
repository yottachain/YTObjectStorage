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
import java.util.List;

@RestController
@RequestMapping(value="/user")
public class UserController {

    private static final Logger LOG = Logger.getLogger(UserController.class);

    @Value("${s3server.fsrepo.root}")
    String fsRepoRoot;
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

    @RequestMapping(value = "/aync",method = RequestMethod.GET)
    @ResponseBody
    public String getAyncUploadStatus() {

        String[] objectList = new File(fsRepoRoot+"/"+syncBucketName).list();

        String status = this.syncStatus(objectList);
        return status;
    }
    public String syncStatus(String[] objectList) {

        //初始化一个list,用来存放所有的xml完整路径
        List<String> list = new ArrayList<>();

        if(objectList.length > 0) {
            for (int i=0;i<objectList.length;i++) {
                File file = new File(fsRepoRoot+"/"+objectList[i]);
                //查询当前bucket下所有的xml文件   xml文件是用来存放文件信息的，一个完整文件对应一个xml文件
                String[] nameList = file.list(((dir, name) -> name.endsWith(".xml") || new File(name).isDirectory()));
                if(nameList.length>0) {
                    for(int ii=0;ii<nameList.length;ii++) {
                        String filePathXml = file.getPath()+"/"+nameList[ii];
                        list.add(filePathXml);
                    }
                }
            }

        }
        if(list.size() == 0) {
            return "SUCCESS";
        } else {
            return ayncInfo(list);
        }
    }

    public String ayncInfo(List<String> list) {
        if(list.size() == 0) {
            return "SUCCESS";
        } else {
            for(int i=0;i<list.size();i++) {
                Path xmlPath = Paths.get(list.get(i));
                if(!Files.exists(xmlPath)) {
                    list.remove(xmlPath);
                }
            }
            if(list.size() > 0) {
                try {
                    Thread.currentThread().sleep(50000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ayncInfo(list);
            }
            return "SUCCESS";
        }
    }
}

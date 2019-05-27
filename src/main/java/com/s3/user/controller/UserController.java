package com.s3.user.controller;

import com.ytfs.client.LocalInterface;
import com.ytfs.common.ServiceException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping(value="/user")
public class UserController {

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

}

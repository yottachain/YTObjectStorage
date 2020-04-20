package com.s3.user.controller;

import com.qcloud.cos.COSClient;
import com.s3.user.controller.sync.task.AyncUploadSenderPool;
import com.s3.user.controller.sync.task.SyncNotice;
import com.ytfs.client.ClientInitor;
import com.ytfs.client.Configurator;
import com.ytfs.client.LocalInterface;
import com.ytfs.client.s3.ObjectHandler;
import com.ytfs.client.v2.YTClient;
import com.ytfs.client.v2.YTClientMgr;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.AESCoder;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import de.mindconsulting.s3storeboot.entities.Ret;
import de.mindconsulting.s3storeboot.entities.YottaUser;
import de.mindconsulting.s3storeboot.util.*;
import io.jafka.jeos.util.KeyUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static de.mindconsulting.s3storeboot.service.CosBackupService.getClient;

@RestController
@RequestMapping(value="/user")
public class UserController {

    private static final Logger LOG = Logger.getLogger(UserController.class);

    @Value("${s3server.fsrepo.root}")
    String fsRepoRoot;
    @Value("${s3server.eosHistoryUrl}")
    String eosHistoryUrl;
    @Value("${s3server.uploadShardThreadNum}")
    int uploadShardThreadNum;
    @Value("${s3server.downloadThread}")
    int downloadThread ;
    @Value("${s3server.PNN}")
    int PNN;
    @Value("${s3server.PTR}")
    int PTR;
    @Value("${s3server.RETRYTIMES}")
    int RETRYTIMES;
    @Value("${s3server.dirctory}")
    String dirctory;
    @Value("${s3server.uploadFileMaxMemory}")
    String setUploadFileMaxMemory;
    @Value("${s3server.uploadBlockThreadNum}")
    String uploadBlockThreadNum;


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

        LOG.info("AYNC TASK ~~~~~~~~~~~~~~~~~~~~~~~~~~");
        SyncNotice sn=new SyncNotice();
        AyncUploadSenderPool.addSyncNotice(sn);

        sn.waitComplete();

        return "SUCCESS";

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
    @RequestMapping(value = "/decrypt",method = RequestMethod.GET)
    @ResponseBody
    public void getDecryptFile(HttpServletRequest request) throws Exception{
        String path = request.getParameter("path");
        String fileName = request.getParameter("filename");
        File file = new File(path+"/"+fileName);
        byte[] data = AESUtil.getBytesFromFile(file);
//        getDecryptFile(data,path,"new_"+fileName);
        AESUtil.getDecryptFile(data,path,"ladon-s3-server.rar");
    }

    @RequestMapping(value = "/getPublicKey",method = RequestMethod.GET)
    @ResponseBody
    public String getPublicKey(HttpServletRequest request,HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");
        String privateKey = request.getParameter("privateKey");
        String publicKey = null;
        try {
            publicKey=KeyUtil.toPublicKey(privateKey).replace("EOS", "YTA");
        }catch (Exception e) {
//            return Ret.error().setData("The private key is wrong 111...");
        }

        String jsonStr = "{\"public_key\":" + "\"" + publicKey + "\"}";
        String result = null;
        try {
            result = HttpClientUtils.ocPost(eosHistoryUrl + "get_key_accounts", jsonStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String ss = "{\"public_key\":" + "\"" + publicKey +",\"names\":" + "\"" + result+ "\"}";
        return ss;
    }


    @RequestMapping(value = "/insertUser",method = RequestMethod.POST)
    @ResponseBody
    public Ret addUser(HttpServletRequest request,HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");
        String privateKey = request.getParameter("privateKey");
        String username = request.getParameter("username");
        String publicKey = null;
        try {
            publicKey=KeyUtil.toPublicKey(privateKey).replace("EOS", "YTA");
        }catch (Exception e) {
            return Ret.error().setData("The private key is wrong 111...");
        }

        String jsonStr = "{\"public_key\":" + "\"" + publicKey + "\"}";
        String result = null;
        try {
            result = HttpClientUtils.ocPost(eosHistoryUrl + "get_key_accounts", jsonStr);
        } catch (Exception e) {
            e.printStackTrace();
            return Ret.error().setData("The private key is wrong 222...");
        }

        JSONObject json = JSONObject.fromObject(result);
        JSONArray array = json.getJSONArray("account_names");
        if(array.size() > 0) {
            //判断用户输入的username是否正确  如果不正确 返回错误
            if(!array.contains(username)) {
                return Ret.error().setData("The username is wrong");
            }
        } else {
            //返回错误  根据当前信息没有查到用户
            return Ret.error().setData("The private key is wrong 333...");
        }


        //*****************用户校验完成**************************
        List<YottaUser> list = new ArrayList<>();
        YottaUser user = new YottaUser();
        user.setUsername(username);
        user.setPrivateKey(privateKey);
        list.add(user);
        YottaUser.save(list);

        try {
            YTClientMgr.newInstance(username,privateKey);
            return Ret.ok();
        } catch (IOException e) {
            e.printStackTrace();
            return Ret.error();
        }
    }

    @RequestMapping(value = "/register",method = RequestMethod.POST)
    @ResponseBody
    public Ret registerUser(HttpServletRequest request,HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");
        String privateKey = request.getParameter("privateKey");
        String username = request.getParameter("username");

        //判断conf目录下是否有cert证书，如果存在不再另外注册，不存在则往下
        Path certpath = Paths.get(dirctory+"/cert");
        if (!Files.exists(certpath)) {
            String publicKey = null;
            try {
                publicKey=KeyUtil.toPublicKey(privateKey).replace("EOS", "YTA");
            }catch (Exception e) {
                return Ret.error().setData("The private key is wrong 111...");
            }

            String jsonStr = "{\"public_key\":" + "\"" + publicKey + "\"}";
            String result = null;
            try {
                result = HttpClientUtils.ocPost(eosHistoryUrl + "get_key_accounts", jsonStr);
            } catch (Exception e) {
                e.printStackTrace();
                return Ret.error().setData("The private key is wrong 222...");
            }

            JSONObject json = JSONObject.fromObject(result);
            JSONArray array = json.getJSONArray("account_names");
            if(array.size() > 0) {
                //判断用户输入的username是否正确  如果不正确 返回错误
                if(!array.contains(username)) {
                    return Ret.error().setData("The username is wrong");
                }
            } else {
                //返回错误  根据当前信息没有查到用户
                return Ret.error().setData("The private key is wrong 333...");
            }
            String aes_name = "yottachain" + username;
            String sha256Key = SHA256Util.getSHA256(aes_name);
            updateAppProperties(sha256Key);




            //以上如果没有问题，则进入下一步  生成证书文件，先将用户名和私钥加密
            AESCoder coder = null;
            try {
                UserConfig.AESKey = KeyStoreCoder.generateUserKey(sha256Key.getBytes());
                coder = new AESCoder(Cipher.ENCRYPT_MODE);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (NoSuchPaddingException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            }
            String combo_box = "{\"privateKey\":" + "\"" + privateKey + "\"" + ",\"username\":" + "\"" + username + "\"}";
            byte[] data = coder.update(combo_box.getBytes());
            String cert_path = dirctory + "/"+"cert";
            File file = new File(cert_path);

            FileOutputStream out = null;
            try {
                out = new FileOutputStream(file);
                out.write(data);
                byte[] data2 = new byte[0];
                try {
                    data2 = coder.doFinal();
                } catch (IllegalBlockSizeException e) {
                    e.printStackTrace();
                } catch (BadPaddingException e) {
                    e.printStackTrace();
                }
                out.write(data2);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                this.init(privateKey,username);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return Ret.ok();
        } else {
            return Ret.error().setData("The private key has been imported. If you need to switch users, please delete cert first, and re-import the certificate after restarting the service");
        }
    }

    @RequestMapping(value = "/deleteCert",method = RequestMethod.DELETE)
    @ResponseBody
    public Ret delete(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");

        Path path = Paths.get(dirctory+"/cert");

        if(Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Ret.ok().setData("Delete certificate complete,Please reimport the private key");
    }


    //用户注册成功后初始化
    private  void init(String KUSp,String username) throws IOException {
        Configurator cfg = new Configurator();
        cfg.setKUSp(KUSp);
        cfg.setUsername(username);
        cfg.setUploadShardThreadNum(uploadShardThreadNum);
        cfg.setDownloadThread(downloadThread);
        cfg.setUploadFileMaxMemory(setUploadFileMaxMemory);
        cfg.setPNN(PNN);
        cfg.setPTR(PTR);
        cfg.setRETRYTIMES(RETRYTIMES);
        cfg.setUploadBlockThreadNum(uploadBlockThreadNum);
        ClientInitor.init(cfg);
    }

    private void updateAppProperties(String sha256_key) {

        String path = dirctory +"/"+ "yts3.conf";

        PropertiesUtil p = new PropertiesUtil(path);

        p.writeProperty("SHA256_KEY",sha256_key);
    }

    @RequestMapping(value = "/get_version",method = RequestMethod.GET)
    @ResponseBody
    public String getVersion(HttpServletRequest request, HttpServletResponse response) {
        String version_info = "{\"version\":\"1.0.0.13\",\"Date\":\"2020-04-08\"}";
        response.setHeader("Access-Control-Allow-Origin","*");
        return version_info;
    }

    @RequestMapping(value = "/get_progress",method = RequestMethod.GET)
    @ResponseBody
    public int getProgress(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");

        String status = ProgressUtil.getUserHDDStatus();
        if("ERR".equals(status)) {
            return 102;
        }
        String bucketName = request.getParameter("bucketName");
        String key = request.getParameter("key");
        boolean isFileExist = false;
        try {
            isFileExist = ObjectHandler.isExistObject(bucketName,key,null);
            LOG.info(key + " is " + isFileExist);
        } catch (ServiceException e) {
            e.printStackTrace();
        }

        String s = bucketName+"-"+key;
        String sha256Key = SHA256Util.getSHA256(s);
        int num = 0;
        try {
            num = ProgressUtil.getProgress(sha256Key);
            if(num ==0 && isFileExist == true) {
                num = 100;
            }
        }catch (Exception e) {
            if(isFileExist) {
                num = 100;
            } else {
                num = 0;
            }

        }

        return num;
    }

    @RequestMapping(value = "/get_ybscan",method = RequestMethod.GET)
    @ResponseBody
    public String get_counts(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");

        String bucketName = "ybscan";
        boolean isVersion = true;
        int maxKeys = 1000000;
        String prefix = "";
        String fileName = "";
        ObjectId nextId = new ObjectId("000000000000000000000000");
        List<FileMetaMsg> fileMetaMsgs = null;
        try {
            fileMetaMsgs = ObjectHandler.listBucket(bucketName,fileName,prefix,isVersion,nextId,maxKeys);
        } catch (ServiceException e) {
            e.printStackTrace();
        }

        int counts = fileMetaMsgs.size();
        Date date = new Date();
        String jsonStr = "{\"file_count\":" + "\"" + counts + "\"" + ",\"Date\":" + "\"" + date + "\"}";
        return jsonStr;
    }


    @RequestMapping(value = "/get_cosLicense",method = RequestMethod.GET)
    @ResponseBody
    public String get_license(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");
        String cos_key="5dc3383e69bd32ca72696e98d31d4c65a4b06c60f962bdf7239d50f2e58ec6c8";
        String path = dirctory + "/" +"license.lic";
        String secretId = request.getParameter("secretId");
        String secretKey = request.getParameter("secretKey");
        String appId=1258989317+"";
        String combo_box = "{\"secretId\":" + "\"" + secretId + "\"" + ",\"secretKey\":" + "\"" + secretKey + "\""+",\"appId\":" + "\"" + appId+"\"}";
        String aes = null;
        try {
            aes = de.mindconsulting.s3storeboot.util.AESCoder.encrypt(cos_key,combo_box);
        } catch (Exception e) {
            e.printStackTrace();
        }

        File file = new File(path);
        byte[] data = aes.getBytes();

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "COS CHANGE SUCCESS";
    }


    @RequestMapping(value = "/create_cos_bucket",method = RequestMethod.POST)
    @ResponseBody
    public void createCosBucket(HttpServletRequest request, HttpServletResponse response){
        response.setHeader("Access-Control-Allow-Origin","*");
        String appId = request.getParameter("appId");
        COSClient cosClient = getClient();
        for(int i=0;i<180;i++) {
            if(i>71) {
                String bucketName="yotta"+i+"-"+appId;
                cosClient.createBucket(bucketName);
                LOG.info("第 "+i+" 个bucket");
                LOG.info("CREATE BUCKET SUCCESS:::"+bucketName);
            }
        }
    }
    @RequestMapping(value = "/get_userInfo",method = RequestMethod.GET)
    @ResponseBody
    public void getUserInfo(){
        int userID = UserConfig.userId;
        String privateKey = UserConfig.privateKey;
        String username = UserConfig.username;
       LOG.info("userID : "+userID+" ,username : "+ username + " ,privateKey : "+ privateKey);
    }
    @RequestMapping(value = "/importUsers",method = RequestMethod.POST)
    @ResponseBody
    public Ret importUsers(HttpServletRequest request,HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin","*");
        List<YottaUser> users = new ArrayList<>();
        Workbook wb = null; //拿到文件
        try {
            Path path = Paths.get("../excel/users.xlsx");
            if(Files.exists(path)){
                wb = WorkbookFactory.create(new File("../excel/users.xlsx"));
            } else {
                return Ret.error().setMsg("Please place users.xlsx in the excel directory.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidFormatException e) {
            e.printStackTrace();
        }
        Sheet sheet = wb.getSheetAt(0);
        int lastRowNum = sheet.getLastRowNum();
        for (int i = 1; i <= lastRowNum; i++) {
            Row row = sheet.getRow(i);
            short lastCellNum = row.getLastCellNum();
           YottaUser user = new YottaUser();
            for (int j = 0; j < lastCellNum; j++) {
                Cell cell = row.getCell(j);
                if(j == 0 && !cell.getStringCellValue().trim().isEmpty()) {
                    user.setUsername(cell.getStringCellValue());
                }
                if(j == 1 && !cell.getStringCellValue().trim().isEmpty()){
                    user.setPrivateKey(cell.getStringCellValue());
                }
            }
            if(user.getPrivateKey() !=null || user.getUsername() != null) {
                users.add(user);
            }

        }
        try {
            wb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<YottaUser> listUser = new ArrayList<>();
        if(users.size() > 0) {

            for(YottaUser yottaUser : users){
                boolean flag = this.validateUser(yottaUser);
                if(!flag) {
                    LOG.error("Please check the user information is correct, username:"+yottaUser.getUsername()+" ,privateKey:"+yottaUser.getPrivateKey());
//                    users.remove(yottaUser);
                } else {
                    listUser.add(yottaUser);
                    try {
                        YTClientMgr.newInstance(yottaUser.getUsername(),yottaUser.getPrivateKey());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            YottaUser.save(listUser);
        }

        LOG.info("Batch import users completed, the number of successful import users is " + listUser.size());
//        return Ret.ok().setMsg("Batch import users completed, the number of successful import users is " + listUser.size());
        return Ret.ok().setData(listUser).setMsg("Batch import users completed, the number of successful import users is " + listUser.size());
    }


    public boolean validateUser(YottaUser user) {
        String publicKey = null;
        try {
            publicKey=KeyUtil.toPublicKey(user.getPrivateKey()).replace("EOS", "YTA");
        }catch (Exception e) {
            return false;
        }
        String jsonStr = "{\"public_key\":" + "\"" + publicKey + "\"}";
        String result = null;
        try {
            result = HttpClientUtils.ocPost(eosHistoryUrl + "get_key_accounts", jsonStr);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        JSONObject json = JSONObject.fromObject(result);
        JSONArray array = json.getJSONArray("account_names");
        if(array.size() > 0) {
            //判断用户输入的username是否正确  如果不正确 返回错误
            if(!array.contains(user.getUsername())) {
                return false;
            }
        } else {
            //返回错误  根据当前信息没有查到用户
            return false;
        }
        return true;
    }
}

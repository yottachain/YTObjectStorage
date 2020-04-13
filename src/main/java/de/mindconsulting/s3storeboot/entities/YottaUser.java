package de.mindconsulting.s3storeboot.entities;

import java.io.*;
import java.io.File;
import java.util.*;

public class YottaUser implements Serializable {

    private static final long serialVersionUID = -4382915422537471820L;

    private String username;

    private String privateKey;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public static void save(List<YottaUser> userlist) {
        List<YottaUser> list = read();
        Map<String,YottaUser> map = new HashMap<>();
        for(YottaUser user : list) {
            map.put(user.getPrivateKey(),user);
        }
        for(YottaUser user : userlist) {
            map.put(user.getPrivateKey(),user);
        }
        List<YottaUser> listAll = new ArrayList<>();
        if(map.size()>0) {
            map.forEach((key,value) ->{
                listAll.add(value);
            });
        }

        try {
            FileOutputStream fs = new FileOutputStream("../conf/cert.list");
            ObjectOutputStream os = new ObjectOutputStream(fs);
            os.writeObject(listAll);
            os.flush();
            os.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static List<YottaUser> read() {
        List<YottaUser> list = new ArrayList<>();
        try{
            File file = new File("../conf/cert.list");
            if(file.length() > 0) {
                FileInputStream fs = new FileInputStream("../conf/cert.list");
                ObjectInputStream ois = new ObjectInputStream(fs);
                list = (List<YottaUser>) ois.readObject();
                ois.close();
            }
        }catch (Exception e) {

        }
        return list;
    }


    public static void main(String[] args){
        List<YottaUser> list = new ArrayList<>();
        YottaUser user1 = new YottaUser();
        user1.setPrivateKey("5KKBMjenzB5BNjvdqARwKhUqHSpyz42B2htxTB3cyEiyfjfW5CU");
        user1.setUsername("zexibaobao21");
        list.add(user1);
        YottaUser user2 = new YottaUser();
        user2.setPrivateKey("5JDriTmKZiU4KGTgTAimC3XvCTpbzKA9nKXxjjJKszATmP44PTc");
        user2.setUsername("xiwangshequ1");
        list.add(user2);
        YottaUser user3 = new YottaUser();
        user3.setPrivateKey("5KWGzBghrSb8YZTbbrH7PEPVJQ46LQjFLMYCsU8CT7WrJThq6eF");
        user3.setUsername("jurenwangluo");
        list.add(user3);
        save(list);
        read();
    }

}

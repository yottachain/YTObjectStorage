package de.mindconsulting.s3storeboot.jaxb.meta;


import de.mindconsulting.s3storeboot.repository.impl.S3StoreUser;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@XmlRootElement(name = "UserMeta")
public class UserData {

    private Map<String, S3StoreUser> users;

    public UserData(Map<String,S3StoreUser> users) {
        this.users = users;
    }

    public UserData() {}

    @XmlElement(name = "users")
    public Map<String, S3StoreUser> getUsers() {
        return users;
    }

    public void setUsers(Map<String, S3StoreUser> users) {
        this.users = users;
    }

}

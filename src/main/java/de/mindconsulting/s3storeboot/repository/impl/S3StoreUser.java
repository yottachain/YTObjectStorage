package de.mindconsulting.s3storeboot.repository.impl;

import de.mc.ladon.s3server.entities.api.S3User;

import java.util.Set;

public class S3StoreUser implements S3User{
    private String userId;
    private String userName;
    private String secretKey;
    private String publicKey;
    private Set<String> roles;

    public S3StoreUser() {}

    public S3StoreUser(String userId, String userName, String publicKey, String secretKey, Set<String> roles) {
        System.out.println("userId======="+userId);
        System.out.println("userName======="+userName);
        this.userId = userId;
        this.userName = userName;
        this.secretKey = secretKey;
        this.publicKey = publicKey;
        this.roles = roles;
    }


    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public String getSecretKey() {
        return secretKey;
    }

    @Override
    public String getPublicKey() {
        return publicKey;
    }

    @Override
    public Set<String> getRoles() {
        return roles;
    }

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }

    @Override
    public String toString() {
        return "S3StoreUser{" +
                "userId='" + userId + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}

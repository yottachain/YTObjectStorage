package de.mindconsulting.s3storeboot.entities;


import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class File {

    protected String _id;

    //此字段是目录表的id
    protected String bucketId;

    //此字段为文件名称
    protected String key;

    //此字段为用户表的VNU
    protected String VNU;
}

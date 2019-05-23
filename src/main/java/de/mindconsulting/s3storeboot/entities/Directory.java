package de.mindconsulting.s3storeboot.entities;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import javax.validation.groups.Default;

@Setter
@Getter
public class Directory {

    @NotNull(message = "目录ID不能为空", groups = {Default.class})
    protected String _id;

    @NotNull(message = "用户ID不能为空", groups = {Default.class})
    protected Integer userId;

    @NotNull(message = "目录名称不能为空")
    protected String bucket;

}

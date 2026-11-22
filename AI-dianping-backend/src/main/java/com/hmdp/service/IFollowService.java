package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;

public interface IFollowService extends IService<Follow> {

    Result follow(Long id, Boolean isFollow);

    Result isFollow(Long id);

    Result followCommons(Long id);
}

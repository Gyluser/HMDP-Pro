package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import jakarta.servlet.http.HttpSession;

public interface IUserService extends IService<User> {

    Result login(LoginFormDTO loginForm, HttpSession session);

    Result sendCode(String phone, HttpSession session);

    Result logout();

    Result sign();

    Result signCount();
}

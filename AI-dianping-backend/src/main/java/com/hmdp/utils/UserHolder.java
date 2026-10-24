package com.hmdp.utils;

import com.hmdp.dto.UserDTO;

public class UserHolder {
    // 不向线程池子线程继承用户，避免线程复用时发生用户串号。
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    public static void saveUser(UserDTO user){
        tl.set(user);
    }

    public static UserDTO getUser(){
        return tl.get();
    }

    public static void removeUser(){
        tl.remove();
    }
}

package com.qs.game.streaming.utils;

import org.apache.commons.lang.StringUtils;

/**
 * Created by zun.wei on 2018/6/26.
 * To change this template use File|Default Setting
 * |Editor|File and Code Templates|Includes|File Header
 */
public class InterfaceUtils {


    public static int getAccessTypeByName(String name) {
        return StringUtils.equals("login.do",name) ? 1 :
                StringUtils.equals("load.do",name) ? 2 :
                StringUtils.equals("getUser.do",name) ? 3 :
                StringUtils.equals("payNotify.do",name) ? 4 :
                StringUtils.equals("commitOrder.do",name) ? 5 :
                StringUtils.equals("getActiList.do",name) ? 6 :
                StringUtils.equals("getAddress.do",name) ? 7 :
                StringUtils.equals("getAwardList.do",name) ? 8 : 9;
    }


    public static String getAccessNameByType(int type) {
        switch (type) {
            case 1 :
                return "登录接口";
            case 2 :
                return "加载数据接口";
            case 3 :
                return "获取用户接口";
            case 4 :
                return "支付回调接口";
            case 5 :
                return "提交订单接口";
            case 6 :
                return "获取活动列表接口";
            case 7 :
                return "获取用户地址接口";
            case 8 :
                return "获取奖品列表接口";
                default:
                    return "未知";
        }
    }


    public static String getStatusInfoByType(int code) {
        switch (code) {
            case 200 :
                return "请求已成功";
            case 404 :
                return "请求失败,资源未发现";
            case 500 :
                return "请求失败,服务器的程序码出错";
            case 403 :
                return "服务器接收请求，但拒绝执行";
            case 307 :
                return "从不同的URI响应请求";
            default:
                return "从不同的URI响应请求";
        }
    }


}

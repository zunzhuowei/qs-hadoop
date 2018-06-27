package com.qs.game.streaming.service;

import com.qs.game.streaming.model.AccessCount;
import com.qs.game.streaming.model.AccessSuccessCount;

import java.util.List;

/**
 * Created by zun.wei on 2018/6/27 19:28.
 * Description:
 */
public interface IAccessService {


    /**
     * 根据时间查找访问日志
     * @param date 日期 格式 20180627
     * @return 列表
     */
    List<AccessCount> getAccessCountListByDate(String date) throws Exception;


    /**
     * 根据时间查找访问成功日志
     * @param date 日期 格式 20180627
     * @return 列表
     */
    List<AccessSuccessCount> getAccessSuccessCountListByDate(String date) throws Exception;



}

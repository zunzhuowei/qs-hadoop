package com.qs.game.streaming.controller;

import com.alibaba.fastjson.JSON;
import com.qs.game.streaming.dao.AccessCountDao;
import com.qs.game.streaming.model.AccessCount;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by zun.wei on 2018/6/27 16:43.
 * Description:
 */
@RestController
@RequestMapping("/log/")
public class AccessLogController {


    @Resource
    private AccessCountDao accessCountDao;

    @PostMapping("/get/{date}")
    public String getDate(@PathVariable("date") String date) throws Exception {
        List<AccessCount> accessCountList = accessCountDao.getAccessCountListByDate(date);
        System.out.println("accessCountList = " + accessCountList);
        return JSON.toJSONString(accessCountList);
    }


}

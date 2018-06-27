package com.qs.game.streaming.controller;

import com.alibaba.fastjson.JSON;
import com.qs.game.streaming.dao.AccessCountDao;
import com.qs.game.streaming.model.AccessCount;
import com.qs.game.streaming.model.AccessSuccessCount;
import com.qs.game.streaming.service.IAccessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static Logger logger = LoggerFactory.getLogger(AccessLogController.class);

    @Resource
    private IAccessService accessService;

    @PostMapping("/get/{date}")
    public String getData(@PathVariable("date") String date) throws Exception {
        List<AccessCount> accessCountList = accessService.getAccessCountListByDate(date);
        logger.info("accessCountList = " + accessCountList);
        return JSON.toJSONString(accessCountList);
    }


    @PostMapping("/get/success/{date}")
    public String getSuccessDate(@PathVariable("date") String date) throws Exception {
        List<AccessSuccessCount> accessSuccessCountList = accessService.getAccessSuccessCountListByDate(date);
        logger.info("accessCountList = " + accessSuccessCountList);
        return JSON.toJSONString(accessSuccessCountList);
    }


}

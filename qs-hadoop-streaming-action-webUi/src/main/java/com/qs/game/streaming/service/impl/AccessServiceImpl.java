package com.qs.game.streaming.service.impl;

import com.qs.game.streaming.dao.AccessCountDao;
import com.qs.game.streaming.dao.AccessSuccessCountDao;
import com.qs.game.streaming.model.AccessCount;
import com.qs.game.streaming.model.AccessSuccessCount;
import com.qs.game.streaming.service.IAccessService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by zun.wei on 2018/6/27 19:28.
 * Description: 访问日志业务层接口实现类
 */
@Service
public class AccessServiceImpl implements IAccessService {


    @Resource
    private AccessSuccessCountDao accessSuccessCountDao;

    @Resource
    private AccessCountDao accessCountDao;

    @Override
    public List<AccessCount> getAccessCountListByDate(String date) throws Exception {
        return accessCountDao.getAccessCountListByDate(date);
    }

    @Override
    public List<AccessSuccessCount> getAccessSuccessCountListByDate(String date) throws Exception {
        return accessSuccessCountDao.getAccessCountListByDate(date);
    }


}

package com.qs.game.streaming.dao;

import com.qs.game.streaming.model.AccessCount;
import com.qs.game.streaming.utils.HBaseUtils;
import com.qs.game.streaming.utils.InterfaceUtils;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by zun.wei on 2018/6/27 18:44.
 * Description:
 */
@Repository
public class AccessCountDao {

    @Resource
    private HBaseUtils hBaseUtils;


    public List<AccessCount> getAccessCountListByDate(String date) throws Exception {
        Map<String, Long> result = hBaseUtils.query("qs_access_log", "info","count", date);
        List<AccessCount> list = new LinkedList<>();
        for (Map.Entry<String, Long> kk : result.entrySet()) {
            AccessCount accessCount = new AccessCount();
            accessCount.setCount(kk.getValue());
            accessCount.setName(kk.getKey());
            list.add(accessCount);
        }
        list = list.stream().peek(e -> {
            String name = e.getName();
            e.setName(InterfaceUtils.getAccessNameByType(Integer.parseInt(name.substring(name.indexOf("_") + 1))));
        }).collect(Collectors.toList());

        return list;
    }


}

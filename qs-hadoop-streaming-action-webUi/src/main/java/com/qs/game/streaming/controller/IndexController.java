package com.qs.game.streaming.controller;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * Created by zun.wei on 2018/6/27 16:23.
 * Description:
 */
@Controller
public class IndexController {


    @GetMapping("")
    public String index() {
        return "echarts/index";
    }

    @GetMapping("{index}")
    public String index(@PathVariable(name = "index",required = false) String index) {
        return StringUtils.equals("1",index) ? "echarts/index" : "echarts/index2";
    }

}

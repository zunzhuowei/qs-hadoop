package com.qs.game.streaming.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

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

}

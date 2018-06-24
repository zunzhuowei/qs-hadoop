package com.qs.game;

import org.apache.log4j.Logger;

/**
 * 日志生成类
 */
public class LogerGenerater {

    public static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(LogerGenerater.class);

    public static Logger logger = Logger.getLogger(LogerGenerater.class.getName());

    public static void main(String[] args) throws InterruptedException {

        int i = 0;
        for (; ; ) {
            LOGGER.info("com.qs.game.LogerGenerater generate value is ---::" + i++);
            Thread.sleep(2000);
        }

    }

}

package com.qs.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by zun.wei on 2018/6/27 12:20.
 * Description: 配置工具类
 */
public class ConfigUtils {

    private static Properties properties = null;


    public static String getPropertyByKey(String key) {
        if (Objects.isNull(properties)) {
            synchronized (ConfigUtils.class) {
                if (Objects.isNull(properties)) {
                    properties = new Properties();
                    // 使用ClassLoader加载properties配置文件生成对应的输入流
                    InputStream in = ConfigUtils.class.getClassLoader().getResourceAsStream("hadoop.properties");
                    // 使用properties对象加载输入流
                    try {
                        properties.load(in);
                        return properties.getProperty(key);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                }else {
                    return properties.getProperty(key);
                }
            }
        }else {
            return properties.getProperty(key);
        }
    }


}

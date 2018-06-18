package com.ggstar.testclient;

import com.ggstar.util.ip.IpHelper;
import org.junit.Test;

/**
 * Created by Wang Zhe on 2015/8/11.
 */
public class Client {

    @Test
    public void example() throws Exception {
        String ip = "58.30.15.255";
        String region = IpHelper.findRegionByIp(ip);
        System.out.println(region);
    }
}

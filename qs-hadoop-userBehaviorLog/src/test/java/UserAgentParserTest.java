import com.alibaba.fastjson.JSON;
import com.qs.user.source.UserAgent;
import com.qs.user.source.UserAgentParser;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by zun.wei on 2018/6/9.
 * To change this template use File|Default Setting
 * |Editor|File and Code Templates|Includes|File Header
 */
public class UserAgentParserTest {



    /**
     * 单机版统计nginx访问日志的osp平台
     * @throws IOException
     */
    @Test
    public void TestOs() throws IOException {
        UserAgentParser userAgentParser = new UserAgentParser();
        String filePath = "C:\\Users\\Administrator\\Desktop\\access.log";
        InputStreamReader inputStreamReader = new InputStreamReader(
                new FileInputStream(new File(filePath))
        );
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line = "";
        int i = 0;
        Map<String, Integer> result = new HashMap<>();
        while (true) {
            line = bufferedReader.readLine();
            if (Objects.isNull(line)) break;
            String [] source = line.split("\"");
            if (source.length < 5) continue;
            //System.out.println("source = " + source[5]);

            UserAgent agent = userAgentParser.parse(source[5]);

            if (Objects.nonNull(agent)) {
                Integer exist = result.get(agent.getOs());
                if (Objects.nonNull(exist)) {
                    result.put(agent.getOs(), exist + 1);
                } else {
                    result.put(agent.getOs(), 1);
                }
            }
            //System.out.println("agent = " + agent);
            i ++;
        }
        System.out.println("i = " + i);
        System.out.println("result = " + JSON.toJSONString(result,Boolean.TRUE));
    }




    /**
     * 测试userAgentParser 解析
     */
    @Test
    public void TestUserAgentParser() {
        //String source = "Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML; like Gecko) Mobile/12F70 MicroMessenger/6.1.5 NetType/WIFI";
        //String source = "Mozilla/5.0 (Linux; Android 6.0.1; OPPO R9sk Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/044103 Mobile Safari/537.36 MicroMessenger/6.6.6.1300(0x26060637) NetType/4G Language/zh_CN";
        //String source = "Dalvik/2.1.0 (Linux; U; Android 8.1.0; vivo X21A Build/OPM1.171019.011)";
        String source = "libcurl";
        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        System.out.println("agent = " + agent);
    }


}

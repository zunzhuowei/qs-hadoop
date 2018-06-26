package com.qs.game;

import org.apache.commons.lang.time.FastDateFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

/**
 * nginx 访问日志生成器
 */
public class NginxAccessLogGenerater {

//120.27.173.48 - - [09/Jun/2018:03:10:35 +0800] "GET /kxrobot/api/shareLink/joinViewUi.html?sesskey=220077-1528484922012-102-52fca2e794103ff1be57fd9d0d2d29d6-0-17&roomid=520829&roomInfo=ICLluILlubMi55qE6Iy26aaGIOiMtummhuWQjTrlpb3lj4vkuIAq5a2X54mMIOiMtummhklEOjExMDg1Nw==&jushu=8&signCode=1528485028&wanfa=54K554Ku5b+F6IOhXznmga/otbfog6Ff6Ieq5pG457+75YCNX+S4ieaBr+S4gOWbpF/kuI3po5g=&sign=858BC71AE83392E884366B6660CC0FF3&roomtitle=6YO05bee5a2X54mM&pType=10 HTTP/1.1" 200 29411 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML; like Gecko) Mobile/12F70 MicroMessenger/6.1.5 NetType/WIFI" "101.227.139.173, 116.211.165.14"

// 2018-06-25 22:36:48  "POST /kxrobot/api/shareLink/cookieJoinRoom.html HTTP/1.1"  200 223.146.243.115

    private static final Random random = new Random();
    private static final String URL_PREFIX = "/app/api/";
    private static final String URL_PREFIX_ACTI = "/acti/api/";

    private static final String POST = "POST";
    private static final String GET = "GET";

    private static final FastDateFormat TARGET_DATE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss", Locale.ENGLISH);

    private static final FastDateFormat SRC_DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);


    private static final List<String> ACCESS_URL_LIST = new ArrayList<>();

    private static final List<Integer> STATUS_LIST = new ArrayList<>();

    static {
        ACCESS_URL_LIST.add(URL_PREFIX + "login.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX + "load.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX + "getUser.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX + "getOrder.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX + "payNotify.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX + "commitOrder.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX_ACTI + "getActiList.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX_ACTI + "getAddress.do?mid=");
        ACCESS_URL_LIST.add(URL_PREFIX_ACTI + "getAwardList.do?mid=");

        STATUS_LIST.add(200);
        STATUS_LIST.add(404);
        STATUS_LIST.add(500);
        STATUS_LIST.add(403);
        STATUS_LIST.add(307);
    }



    public static String getAccessLog(int lines) {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < lines; i++) {
            Date nowDate = new Date();
            stringBuilder.append(SRC_DATE_FORMAT.format(nowDate)).append("\t");
            stringBuilder.append("\"");

            int method = random.nextInt(2);
            stringBuilder.append(method == 1 ? POST : GET).append(" ");
            int accessUrl = random.nextInt(9);
            stringBuilder.append(ACCESS_URL_LIST.get(accessUrl));

            int mid = random.nextInt(10);
            int mid1 = random.nextInt(10);
            int mid2 = random.nextInt(10);
            int mid3 = random.nextInt(10);
            int mid4 = random.nextInt(10);

            stringBuilder.append(mid).append(mid1).append(mid2).append(mid4).append(mid3);
            stringBuilder.append(" ").append("HTTP/1.1");
            stringBuilder.append("\"").append("\t");
            int status = random.nextInt(5);
            stringBuilder.append(STATUS_LIST.get(status)).append("\t");

            int ip1 = random.nextInt(1000);
            int ip2 = random.nextInt(1000);
            int ip3 = random.nextInt(1000);
            int ip4 = random.nextInt(1000);

            stringBuilder.append(ip1).append(".")
                    .append(ip2).append(".")
                    .append(ip3).append(".")
                    .append(ip4);
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }



    public static void WriteStringToFile(String filePath,String lines) {
        File file = null;
        PrintStream ps = null;
        try {
            file = new File(filePath);
            ps = new PrintStream(new FileOutputStream(file,true));
            //ps.println("http://www.jb51.net");// 往文件里写入字符串
            ps.append(lines);// 在已有的基础上添加字符串
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            if (ps != null) {
                ps.close();
            }
        }
    }


    public static void generateAccessLog(int lines) {
        String logs = NginxAccessLogGenerater.getAccessLog(lines);
        NginxAccessLogGenerater.WriteStringToFile("access.log", logs);
    }


    public static void main(String[] args) throws InterruptedException {
        if(args.length != 2){
            System.out.println("NginxAccessLogGenerater usage " +
                    "parama <lines : int> <seconds : int>");
            System.exit(1);
        }

        for (; ; ) {
            NginxAccessLogGenerater.generateAccessLog(Integer.parseInt(args[0]));
            System.out.println("NginxAccessLogGenerater Every "
                    + args[1] + " seconds generate "
                    + args[0] + " lines access log"
                    +",log name is access.log");
            Thread.sleep(1000 * Integer.parseInt(args[1]));
        }
    }

}

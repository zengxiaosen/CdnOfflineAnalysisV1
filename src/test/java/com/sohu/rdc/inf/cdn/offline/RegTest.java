package com.sohu.rdc.inf.cdn.offline;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yunhui li on 2017/5/18.
 */
public class RegTest {
    public static void main(String[] args) {


//        String log = "04:30:25 local_nginx@bx_20_57 1494880225.091 0.001 110.254.168.73 -/200 199" +
//            " GET http://api.k.sohu.com/api/pushsdk/p.go?rt=json&v=%7B%22action%22%3A%227%22%2C%22pid%22%3A1%2C%22pushId%22%3A%227609f1b41446786387aae6e3dc4243004be1b525%22%2C%22type%22%3A%220%22%2C%22authOk%22%3A0%7D&m=B57ajkfIG7-RlnR2fFDmLw" +
//            " - DIRECT/10.13.89.93:80(0.001) application/octet-stream \"-\" \"Dalvik/1.6.0 " +
//            "(Linux; U; Android 4.4.4; TCL P502U Build/KTU84P)\"";
        String log = "local_nginx@yd_88_54 1494878910.247 0.001 36.149.167.206 -/200 9790 GET" +
            " " +
            "http://changyan.sohu.com/api/2/topic/comments?page_size=10&style&page_no=1&client_id=cyrwy0ixu&topic_id=398678086&sub_size=0&depth=0&order_by - DIRECT/10.16.43.169:80(0.001)" +
            " application/json; sdf; dfg; sdfa \"-\" \"okhttp/2.6.0\"";

        String regex = "(.*\\d+:\\d+:\\d+)? ?(\\S+[@| ]\\S+) (\\d+.?\\d+) (\\d+.?\\d+)" +
            " (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (-/\\d+) (\\d+) (\\w{3,6})" +
            " (\\w{3,5}://\\S+) - (\\S+) (\\S+[;|; ]?[^\"]*?[;|; ]?[^\"]*?)" +
            " ([\"]\\S*[\"])( [\"].*[\"])?";

        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(log);

        if(matcher.find()){
            System.out.println("find");
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));
            System.out.println(matcher.group(5));
            System.out.println(matcher.group(6));
            System.out.println(matcher.group(7));
            System.out.println(matcher.group(8));
            System.out.println(matcher.group(9));
            System.out.println(matcher.group(10));
            System.out.println(matcher.group(11));
            System.out.println(matcher.group(12));
            System.out.println(matcher.group(13));
        }else{
            System.out.println("no match");
        }

//        System.out.println(matcher.matches());
//        matcher.reset();
//
//        while (matcher.find()) {
//
//        }
    }
}

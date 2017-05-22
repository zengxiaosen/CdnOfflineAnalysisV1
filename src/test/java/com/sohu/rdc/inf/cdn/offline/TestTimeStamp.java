package com.sohu.rdc.inf.cdn.offline;

import org.joda.time.DateTime;

import java.util.Calendar;

/**
 * Created by yunhui li on 2017/5/15.
 */
public class TestTimeStamp {
    public static void main(String[] args) {

//        double tsInSecond = 1484736600.025;
//        double tsInSecond = 1484668800;
//        double tsInSecond = 1494879207.717;
//        double tsInSecond = 1494879207.717;
        double tsInSecond = 1494889207.717;

        int inteval_5m = 5 * 60;

        long roundTs_5m = (long) ((Math.floor(tsInSecond / inteval_5m)) * inteval_5m * 1000);


        DateTime dt_5m = new DateTime(roundTs_5m);
        System.out.println("roundTs_5m=" + roundTs_5m);
        System.out.println(dt_5m.getYear());
        System.out.println(dt_5m.getMonthOfYear());
        System.out.println(dt_5m.getDayOfMonth());
        System.out.println(dt_5m.getHourOfDay());
        System.out.println(dt_5m.getMinuteOfHour());
        System.out.println(dt_5m.getMillis());

        System.out.println(dt_5m.dayOfMonth().roundFloorCopy());
        System.out.println(dt_5m.dayOfMonth().roundFloorCopy().getMillis());
    }
}

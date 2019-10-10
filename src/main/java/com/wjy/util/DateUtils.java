package com.wjy.util;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

public class DateUtils {//输入文件日期时间格式
    //10/Nov/2016:00:01:02 +0800
    private static FastDateFormat YYYYMMDDHHMM_TIME_FORMAT = null;
    private static FastDateFormat TARGET_FORMAT=null;
    static {
        YYYYMMDDHHMM_TIME_FORMAT= FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        TARGET_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    }


    /**
     * 获取时间：yyyy-MM-dd HH:mm:ss
     */
    public static String parse(String time)  {
        String format = TARGET_FORMAT.format(new Date(getTime(time)));
        return format;
    }

    /**
     * 获取输入日志时间：long类型
     *
     * time: [10/Nov/2016:00:01:02 +0800]
     */
    public static long  getTime(String time)  {
        try {
            long time1 = YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
                    time.lastIndexOf("]"))).getTime();
            return time1;
        } catch  (ParseException e) {
            e.printStackTrace();
            return 0l;
        }
    }


    public static void main(String[] args) {
        String parse = parse("[10/Nov/2016:00:01:02 +0800]");
        System.out.println(parse);
    }
}

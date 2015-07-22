package org.jerry.bidata.hclient.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by leone on 6/15/14.
 */
public class TimeUtils {


    /**
     * 时间戳        forCOMMITtime
     *
     * @param dateStringwithTimeZone
     * @return
     * @throws java.text.ParseException
     */
    public static long getTime(String dateStringwithTimeZone) throws ParseException {
        DateTimeFormatter parser = ISODateTimeFormat.dateTime();
        DateTime dt = parser.parseDateTime(dateStringwithTimeZone + "+00:00");
        DateTime dt3 = dt.withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai")));
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS");
        String normalCmitTime = formatter.print(dt3);
        return Long.parseLong(normalCmitTime);
    }

    /**
     * 2014-03-31 23:59:59.514000
     *
     * @param dateStringwithTimeZone
     * @return
     * @throws java.text.ParseException timestamp
     */
    public static String getTimeStamp(String dateStringwithTimeZone) throws ParseException {
        DateTimeFormatter parser = ISODateTimeFormat.dateTime();
        DateTime dt = parser.parseDateTime(dateStringwithTimeZone + "+08:00");
        DateTime dt3 = dt.withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai")));
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        String normalCmitTime = formatter.print(dt3);
        return normalCmitTime;
    }

    /**
     * 2014-03-31 23:59:59.514000
     *
     * @param dateStringwithTimeZone
     * @return
     * @throws java.text.ParseException timestamp
     */
    public static String getTimeStampMINUS8(String dateStringwithTimeZone) throws ParseException {
        String first = dateStringwithTimeZone.substring(0, 19);
        String last = dateStringwithTimeZone.substring(19, 26);
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = formatter.parse(first);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR, -8);
        Date date1 = calendar.getTime();
        return formatter.format(date1) + last;
    }

    public static void main1(String[] args) throws ParseException {
        //                System.out.println(a.substring(0,10));
        //                System.out.println(a.substring(11,a.length()));

//        //2014-07-30-05.52.43.000000
//        String a = "2014-07-30-05.52.43.000000";
//        String b =  a.substring(0,10) + " "+a.substring(11,a.length());
//        System.out.println(b);
//        System.out.println(a.substring(0,10));
//        System.out.println(a.substring(11, a.length()));


        System.out.println(getTime("2014-06-13T10:16:36.234401"));
        System.out.println(getTimestamp("2014-06-13 10:16:36.234401"));
    }

    public static String getTimeGap(Date begin, Date end) {
        //    SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long between = 0;
        try {
            //        begin = dfs.parse("2009-07-10 10:22:21.214");
            //        end = dfs.parse("2009-07-20 11:24:49.145");
            between = (end.getTime() - begin.getTime());// 得到两者的毫秒数
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        long day = between / (24 * 60 * 60 * 1000);
        long hour = (between / (60 * 60 * 1000) - day * 24);
        long min = ((between / (60 * 1000)) - day * 24 * 60 - hour * 60);
        long s = (between / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
        long ms = (between - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000
                - min * 60 * 1000 - s * 1000);
        return (day + "天" + hour + "小时" + min + "分" + s + "秒" + ms
                + "毫秒");
    }


    public static String getlastDay() {
        Date today = new Date();
        Date yesterday = getlastDay(today);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String str = sdf.format(yesterday);
        return str;
    }

    public static Date getlastDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        date = calendar.getTime();
        return date;
    }


    /**
     * 需要特殊处理，必须重构，逻辑混乱
     *
     * @param time
     * @return
     */
    public static Timestamp getTimestamp(String time) {
        if (time.contains("T")) {
            try {
                time = getTimeStamp(time);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        if (null == time || "".equals(time)) {
            return null;
        } else {
            return Timestamp.valueOf(time);
        }
    }

    public static String getnextDay(String startDate) throws ParseException {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = formatter.parse(startDate);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, +1);
        date = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String str = sdf.format(date);
        return str;
    }

    private static final int DATE_STRING_LENGTH = "2014-12-01 00:00:00.".length();

    public static String makeTimeRegular(String time) {

        if (time == null) {
            return null;
        } else if (CheckDateFormat.isValidDate(time)) {
            return time;
        } else if (time.length() > DATE_STRING_LENGTH) {

//            System.out.println("6 -(time.length() - DATE_STRING_LENGTH) : " + (6 -(time.length() - DATE_STRING_LENGTH)));
            int timeLen = time.length();
            for (int i = 0; i < 6 - (timeLen - DATE_STRING_LENGTH); ++i) {
                time = time + "0";
            }
            return time;
        }
        if (time.length() == DATE_STRING_LENGTH) {
            time = time + "000000";
            return time;
        } else if (CheckDateFormat.isValidDateSpecifyFormat("yyyy-MM-dd HH:mm:ss", time)) {
            return time + ".000000";
        }

        return time;
    }

    public static String makeTimeRegular(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }
        return makeTimeRegular(timestamp.toString());
    }


    public static void main(String[] args) throws ParseException {
        System.out.println(getTimestamp("2014-06-03T08:57:06.000001"));
        DateTime dt = new DateTime("2014-06-03T08:57:06.000001");
//        DateTime dt3 = dt.withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai")));
        // Date d = c.getTime();
        // long m = dt.getMillis();
        // Date date = new Date(m);
        DateTimeFormatter parser = ISODateTimeFormat.dateTime();// .dateHourMinuteSecondFraction();
        // //.dateTimeParser();
//        DateTime dt2 = parser.parseDateTime("2014-06-03T08:57:06.000001");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        String normalCmitTime = formatter.print(dt);
        System.out.println("n : " + normalCmitTime);

        if (CheckDateFormat.isValidDate("2014-12-01 00:00:00.000")) {
            System.out.println("===================");
        } else {
            System.out.println("nnnnnnn");
        }
        System.out.println(makeTimeRegular("2014-02-02 00:00:00.0"));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00.00"));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00.000"));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00.0000"));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00.00000"));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00.000000"));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00."));
        System.out.println(makeTimeRegular("2014-02-02 00:00:00"));
    }

}

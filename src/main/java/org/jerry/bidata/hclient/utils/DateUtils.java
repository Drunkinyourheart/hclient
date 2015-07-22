package org.jerry.bidata.hclient.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * 日常工具类
 *
 * @author yangyang.fu
 * @date 2014年11月25日 下午1:50:40
 */
public class DateUtils {
    private DateUtils() {
    }

    //  public static final String PATTERN_TIME_E = "yyyy-MM-dd HH:mm:ss.E";
    public static final String PATTERN_MILLSECOND = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String PATTERN_TIME = "yyyy-MM-dd HH:mm:ss";
    public static final String PATTERN_DATE = "yyyy-MM-dd";

    /**
     * @param dateStr
     * @param pattern
     * @return 把时间字符串按模式转成时间
     * @throws java.text.ParseException
     */
    public static Date parseDate(String dateStr, String pattern)
            throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(dateStr);
    }

    public static String parseStr(Date date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static Date addMinute(Date date, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, minute);
        return calendar.getTime();
    }

    public static Date addDate(Date date, int count) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, count);
        return calendar.getTime();
    }

    /**
     * @param date
     * @return 返回入参时间把分秒置为0的时间串，然后小时数据加1
     */
    public static String getEndTime(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.add(Calendar.HOUR, 1);
        Date hourDate = cal.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(hourDate);
    }

    /**
     * @param date
     * @return 返回入参时间把分秒置为0的时间串
     */
    public static String getStartTime(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        Date hourDate = cal.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(hourDate);
    }

    public static long getTime(String dateStringwithTimeZone) throws ParseException {
        DateTimeFormatter parser = ISODateTimeFormat.dateTime();
        DateTime dt = parser.parseDateTime(dateStringwithTimeZone + "+00:00");
        DateTime dt3 = dt.withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai")));
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS");
        String normalCmitTime = formatter.print(dt3);
        return Long.parseLong(normalCmitTime);
    }

    public static void main(String[] args) throws ParseException {
        String hour = "2019-10-09 10";
        Date hourDate1 = DateUtils.parseDate(hour, "yyyy-MM-dd HH");
        Date hourDate2 = parseDate(hour, "yyyy-MM-dd HH");
        System.out.println(hourDate1);
        System.out.println(hourDate2);
        System.out.println(DateUtils.parseStr(new Date(), DateUtils.PATTERN_MILLSECOND));
        System.out.println(DateUtils.parseStr(new Date(), DateUtils.PATTERN_MILLSECOND + "000"));
        System.out.println(DateUtils.parseStr(new Date(), DateUtils.PATTERN_MILLSECOND + "SSS"));

    }
}

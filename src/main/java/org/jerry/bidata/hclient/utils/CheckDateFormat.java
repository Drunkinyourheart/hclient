package org.jerry.bidata.hclient.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CheckDateFormat {

    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";

    public CheckDateFormat() {
    }

    public static int daysBetween(Date smdate, Date bdate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        smdate = sdf.parse(sdf.format(smdate));
        bdate = sdf.parse(sdf.format(bdate));
        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / 86400000L;
        return Integer.parseInt(String.valueOf(between_days));
    }

    public static int daysBetween(String smdate, String bdate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(smdate));
        long time1 = cal.getTimeInMillis();
        cal.setTime(sdf.parse(bdate));
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / 86400000L;
        return Integer.parseInt(String.valueOf(between_days));
    }

    private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);

    public static boolean isValidDate(String inDate) {
        if (inDate == null) {
            return false;
        } else {
//            SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
            if (inDate.trim().length() != defaultDateFormat.toPattern().length()) {
                return false;
            } else {
                defaultDateFormat.setLenient(false);
                try {
                    defaultDateFormat.parse(inDate.trim());
                    return true;
                } catch (ParseException var3) {
                    return false;
                }
            }
        }
    }

    public static boolean isValidDateSpecifyFormat(String formate, String inDate) {
        if (inDate == null) {
            return false;
        } else {
            SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
            if (inDate.trim().length() != dateFormat.toPattern().length()) {
                return false;
            } else {
                dateFormat.setLenient(false);

                try {
                    dateFormat.parse(inDate.trim());
                    return true;
                } catch (ParseException var3) {
                    return false;
                }
            }
        }
    }


    public static void main(String[] args) throws ParseException {
        System.out.println(daysBetween((String) "2012-09-08 00:00:00.000000", (String) "2012-09-15 00:00:00.000000"));
    }

    public static void main1(String[] args) {
        System.out.println(isValidDate("2004-02-29"));
        System.out.println(isValidDate("200502-27"));
    }
}

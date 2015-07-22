package org.jerry.bidata.hclient.bootstrap;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Jerry Deng
 * @date 7/22/15.
 */
public class T {
    public static void main(String[] args) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMddHHmmssSSSSSS");
        String s = simpleDateFormat.format(new Date());
        System.out.println(s);
    }
}

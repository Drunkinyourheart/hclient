package org.jerry.bidata.hclient.bootstrap;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.jerry.bidata.hclient.hbase.HBaseUtils;
import org.jerry.bidata.hclient.hbase.HClient;
import org.jerry.bidata.hclient.hbase.HClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

/**
 * @author Jerry Deng
 * @date 6/23/15.
 */
public class BootStrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) throws Exception {
//        if (args.length < 2) {
//            usageError();
//        }

        String table = args[0];
        String key = args[1];
        boolean isMD5 = Boolean.parseBoolean(args[2]);

//        return getData(table, key, isMD5);
//        return getData(table, key, isMD5);
        System.out.println(" result =================================================================");

        Map<byte[], byte[]> rs = getData(table, key, isMD5);
        for (byte[] keyByte : rs.keySet()) {
            System.out.println(new String(keyByte) + " : " + new String(rs.get(keyByte)));
        }

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        List<Map<byte[], byte[]>> rss = getMoreVersionData(table, key, isMD5);
        for (Map<byte[], byte[]> map : rss) {
            for (byte[] keyByte : rs.keySet()) {
                System.out.println(new String(keyByte) + " : " + new String(rs.get(keyByte)));
            }
            System.out.println("___________________________________________________________________________________________");
        }
        System.out.println(" result =================================================================");
    }

    private static Map<byte[], byte[]> getData(String table, String key, boolean isOK) throws Exception {
        HClient h = null;
        try {
            h = HClientFactory.builder().connectString("yp-name01, yp-name02, yp-data01,yp-data07, yp-data08").table(table.getBytes()).build();

        } catch (TableNotFoundException e) {
            e.printStackTrace();
            System.out.println("=====================");
            System.out.println(e);
            System.out.println("=====================");
        }


        if (isOK) {

            return h.getSingleRowWithMD5Key(key);
        } else {
            return h.getSingleRowWithOriginalKey(key);
        }

    }

    private static List<Map<byte[], byte[]>> getMoreVersionData(String table, String key, boolean isOK) throws Exception {
        HClient h = null;
        try {
            h = HClientFactory.builder().connectString("yp-name01, yp-name02, yp-data01,yp-data07, yp-data08").table(table.getBytes()).build();

        } catch (TableNotFoundException e) {
            e.printStackTrace();
            System.out.println("=====================");
            System.out.println(e);
            System.out.println("=====================");
        }


        if (isOK) {

            return h.getMoreVersionLineWithByteRowKey(HBaseUtils.transfer2MD5(key));
        } else {
//            return h.getSingleRowWithOriginalKey(key);
            return null;
        }

    }


    private static void usageError() {

        System.out.println("Please provice 2 args: sql and hbase table Name");
        ;
    }


}

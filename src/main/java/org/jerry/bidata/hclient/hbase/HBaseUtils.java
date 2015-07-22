package org.jerry.bidata.hclient.hbase;

import com.sun.istack.NotNull;
import org.apache.hadoop.hbase.util.Bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HBaseUtils {

    public static byte[] transfer2MD5(@NotNull String id) throws NoSuchAlgorithmException {
        return Bytes.add(Bytes.toBytes(Md5(id).hashCode()), Bytes.toBytes(id));
    }

    private static String Md5(String plainText) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(plainText.getBytes());
        byte b[] = md.digest();
        int i;
        StringBuffer buf = new StringBuffer("");
        for (int offset = 0; offset < b.length; offset++) {
            i = b[offset];
            if (i < 0)
                i += 256;
            if (i < 16)
                buf.append("0");
            buf.append(Integer.toHexString(i));
        }
        // result = buf.toString();                    //md5 32bit
        // result = buf.toString().substring(8, 24))); //md5 16bit
        String result = buf.toString().substring(8, 24);
        //            System.out.println("mdt 16bit: " + buf.toString().substring(8, 24));
        //            System.out.println("md5 32bit: " + buf.toString());

        return result;
    }

}

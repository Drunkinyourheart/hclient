import com.google.common.collect.Maps;
import org.jerry.bidata.hclient.hbase.HClient;
import org.jerry.bidata.hclient.hbase.HClientFactory;
import org.apache.hadoop.hbase.TableNotFoundException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * @author Jerry Deng
 * @date 6/24/15.
 */
public class T {

    public static void main(String[] args) {

        HClient h = null;
        try {
            h = HClientFactory.builder().connectString("yp-name01, yp-name02, yp-data01,yp-data07, yp-data08").table("ATABLE".getBytes()).build();
        } catch (TableNotFoundException e) {
            e.printStackTrace();
            System.out.println("=====================");
            System.out.println(e);
            System.out.println("=====================");
        }


        for (int i = 0; i < 10; ++i) {

            Map<String, String> line = Maps.newHashMap();
            line.put("ID", "00000" + i);
            line.put("name", "dzy");
            line.put("sex", "boy");

            try {
                h.putRow(line);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
    }

}

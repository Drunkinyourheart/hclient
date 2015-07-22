package org.jerry.bidata.hclient.hbase;

import com.sun.istack.NotNull;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Jerry Deng
 * @date 6/23/15.
 */
public class HClient2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(HClient2.class);
    private String zk;
    private HBaseClient hBaseClient;

    public HClient2(String zk) {
        this.zk = zk;
        this.hBaseClient = new HBaseClient(zk);
    }

    public void put(@NotNull String table, @NotNull Map<String, String> line) {
        byte[][] qua = new byte[line.size()][];
        byte[][] val = new byte[line.size()][];
        String rowKey = null;
        int offset = 0;
        for (Map.Entry<String, String> entry : line.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            qua[offset] = key.getBytes();
            qua[offset++] = value.getBytes();
            if (key.equalsIgnoreCase("ID")) {
                rowKey = key;
            }
        }
        if (rowKey == null) {
            LOGGER.info("the line {} has't id.", line);
            return;
        }
        PutRequest putRequest = new PutRequest(table.getBytes(), rowKey.getBytes(), HBaseConstants.columnFamilyByteArray, qua, val);
        hBaseClient.put(putRequest);
    }

}

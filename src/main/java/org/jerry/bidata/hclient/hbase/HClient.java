package org.jerry.bidata.hclient.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.codehaus.jackson.map.ObjectMapper;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Jerry Deng
 */
public final class HClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HClient.class);
    private HTableInterface hTableInterface;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    public HClient(HTableInterface hTableInterface) throws IOException {
        if (hTableInterface == null) {
            throw new RuntimeException("null hTableInterface.");
        }
        this.hTableInterface = hTableInterface;
//        hTableInterface.setAutoFlush(false);
//        hTableInterface.setWriteBufferSize(6 * 1024 * 1024);
    }

    /**
     * get single row
     *
     * @param rowKey
     * @return
     * @throws Exception
     */
    public Map<byte[], byte[]> getLingWithByteRowKey(byte[] rowKey) throws Exception {

        Get get = new Get(rowKey);
        Result result = hTableInterface.get(get);
        if (result == null || result.isEmpty()) {
            return new HashMap<byte[], byte[]>();
        }

        Map<byte[], byte[]> line = Maps.newHashMap();
        for (org.apache.hadoop.hbase.KeyValue cell : result.raw()) {
            byte[] qualifier = cell.getQualifier();
            byte[] value = cell.getValue();
            line.put(qualifier, value);
        }
        return line;
    }

    /**
     * get single row
     *
     * @param rowKey
     * @return
     * @throws Exception
     */
    public List<Map<byte[], byte[]>> getMoreVersionLineWithByteRowKey(byte[] rowKey) throws Exception {

        List<Map<byte[], byte[]>> lines = Lists.newArrayList();
        for (int e = 1; e <= 10; ++e) {
            Get get = new Get(rowKey);
            get.setMaxVersions(e);
            Result result = hTableInterface.get(get);
            if (result == null || result.isEmpty()) {
                return new LinkedList<Map<byte[], byte[]>>();
            }

            Map<byte[], byte[]> line = Maps.newHashMap();
            boolean isFirst = true;
            for (org.apache.hadoop.hbase.KeyValue cell : result.raw()) {
                if (isFirst) {
                    isFirst = !isFirst;
                }
                byte[] qualifier = cell.getQualifier();
                byte[] value = cell.getValue();
                if (new String(qualifier).equals("ORDER_STATUS")) {
                    System.out.println("tm : " + cell.getTimestamp());
                    System.out.println("val : " + new String(value));
                }
                line.put(qualifier, value);
            }
            lines.add(line);
        }
        return lines;
    }


    public Map<byte[], byte[]> getSingleRowWithOriginalKey(String rowKey) throws Exception {
        return getLingWithByteRowKey(rowKey.getBytes());
    }

    public Map<byte[], byte[]> getSingleRowWithMD5Key(String rowKey) throws Exception {
        return getLingWithByteRowKey(HBaseUtils.transfer2MD5(rowKey));
    }

    /**
     * insert A Map
     *
     * @param line
     * @throws IOException
     */
    public void putRow(Map<String, String> line) throws IOException, NoSuchAlgorithmException {
        Put put = buildPutFromMap(line);
        hTableInterface.put(put);
    }

    public void putBatchRows(List<Map<String, String>> lines) throws IOException, NoSuchAlgorithmException {
        List<Put> putList = Lists.newArrayList();
        for (Map<String, String> line : lines) {
            putList.add(buildPutFromMap(line));
        }
        hTableInterface.put(putList);
    }

    /**
     * insert Maps
     *
     * @param lines
     * @throws IOException
     */
    public void putRows(List<Map<String, String>> lines) throws IOException, NoSuchAlgorithmException {
        List<Put> puts = Lists.newLinkedList();
        for (Map<String, String> line : lines) {
            puts.add(buildPutFromMap(line));
        }
        hTableInterface.put(puts);
    }

    private Put buildPutFromMap(Map<String, String> line) throws NoSuchAlgorithmException {

        if (line.get("ID") == null) {
            LOGGER.info("the line has't id : " + line);
            return null;
        }
        /**
         *       rowKey 在原始表中 为 md5 值。
         */
        Put put = new Put(HBaseUtils.transfer2MD5(line.get("ID")));
        long now = Long.parseLong(simpleDateFormat.format(new Date()));
        for (Map.Entry<String, String> entry : line.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            byte[] valArray = null;
            // null 值不存储
            if (val == null) {
                continue;
            }
            valArray = val.getBytes();
            put.add(HBaseConstants.columnFamilyByteArray, key.getBytes(), now, valArray);
        }
        return put;
    }


    public List<Map<byte[], byte[]>> scan(byte[] byteStartRowKey, byte[] byteStopRowKey) throws IOException {

        Scan scan = new Scan();
        scan.setStartRow(byteStartRowKey);
        scan.setStopRow(byteStopRowKey);

        List<Map<byte[], byte[]>> lines = Lists.newLinkedList();


        ResultScanner resultScanner = null;
        resultScanner = hTableInterface.getScanner(scan);

//        try {
        for (Result result : resultScanner) {
            Map<byte[], byte[]> line = Maps.newHashMap();
            for (org.apache.hadoop.hbase.KeyValue cell : result.raw()) {
                byte[] qualifier = cell.getQualifier();
                byte[] value = cell.getValue();
                line.put(qualifier, value);
            }
            lines.add(line);
        }
        return lines;
    }

    public List<Map<byte[], byte[]>> scanWithFilter(byte[] byteStartRowKey, byte[] byteStopRowKey, List<String> conditions) throws Exception {

        List<Filter> filters = new ArrayList<Filter>();

        if (conditions != null && !conditions.isEmpty()) {

            for (String cond : conditions) {

                LOGGER.info("conditon : " + cond);
                // q_v_v array contains 3 fileds : qualifier, conditon, value
                String[] keyConditionValue = cond.split(",");
                if (keyConditionValue.length != 3) {
                    throw new Exception("过滤条件参数错误");
                }

                // LESS_OR_EQUAL LESS EQUAL GREATER GREATER_OR_EQUAL NOT_EQUAL
                if (keyConditionValue[1].equals("LESS")) {
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter(
                                    // qualifier condition value
                                    "f".getBytes(), keyConditionValue[0].getBytes(), CompareFilter.CompareOp.LESS,
                                    keyConditionValue[2].getBytes());
                    filters.add(singleColumnValueFilter);
                } else if (keyConditionValue[1].equals("LESS_OR_EQUAL")) {
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter("f".getBytes(), keyConditionValue[0].getBytes(),
                                    CompareFilter.CompareOp.LESS_OR_EQUAL, keyConditionValue[2].getBytes());
                    filters.add(singleColumnValueFilter);
                } else if (keyConditionValue[1].equals("EQUAL")) {
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter("f".getBytes(), keyConditionValue[0].getBytes(),
                                    CompareFilter.CompareOp.EQUAL, keyConditionValue[2].getBytes());
                    filters.add(singleColumnValueFilter);
                } else if (keyConditionValue[1].equals("GREATER_OR_EQUAL")) {
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter("f".getBytes(), keyConditionValue[0].getBytes(),
                                    CompareFilter.CompareOp.GREATER_OR_EQUAL, keyConditionValue[2].getBytes());
                    filters.add(singleColumnValueFilter);
                } else if (keyConditionValue[1].equals("GREATER")) {
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter("f".getBytes(), keyConditionValue[0].getBytes(),
                                    CompareFilter.CompareOp.GREATER, keyConditionValue[2].getBytes());
                    filters.add(singleColumnValueFilter);
                } else if (keyConditionValue[1].equals("NOT_EQUAL")) {
                    SingleColumnValueFilter singleColumnValueFilter =
                            new SingleColumnValueFilter("f".getBytes(), keyConditionValue[0].getBytes(),
                                    CompareFilter.CompareOp.NOT_EQUAL, keyConditionValue[2].getBytes());
                    filters.add(singleColumnValueFilter);
                }
            } // for
        } // if

        Scan scan = new Scan();
        if (byteStartRowKey != null) {
            scan.setStartRow(byteStartRowKey);
        }
        if (byteStopRowKey != null) {
            scan.setStopRow(byteStopRowKey);
        }
        if (filters != null && !filters.isEmpty()) {
            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
        }

        ResultScanner resultScanner = null;
        List<Map<byte[], byte[]>> lines = Lists.newLinkedList();
        resultScanner = hTableInterface.getScanner(scan);

        try {
            for (Result result : resultScanner) {
                Map<byte[], byte[]> line = new HashMap<byte[], byte[]>();
                // 一行的处理, 提取信息，获取一行
//                cdh5.3.x
//                for (org.apache.hadoop.hbase.Cell cell : result.rawCells()) {  //
//                    line.put(cell.getQualifierArray(), cell.getValueArray());
//                }
//                cdh4.6
                for (org.apache.hadoop.hbase.KeyValue keyValue : result.raw()) {
                    line.put(keyValue.getQualifier(), keyValue.getValue());
                }

            }
        } finally {
            resultScanner.close();
            hTableInterface.close();
        }
        return lines;
    }


    private static void checkQualifier(byte[] array) {
        if (((long) array.length & -268435456L) != 0L) {
            if (array.length < 0) {
                throw new AssertionError("Negative byte array length: " + array.length + ' ' + Bytes.pretty(array));
            } else {
                throw new IllegalArgumentException("Byte array length too big: " + array.length + " > " + 268435455L);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        HClient h = HClientFactory.builder().connectString("yp-name01, yp-name02, yp-data01").table("O_P_NEW".getBytes()).build();
        Map<byte[], byte[]> map = h.getLingWithByteRowKey(HBaseUtils.transfer2MD5("18344707")); // 18344707

        for (byte[] key : map.keySet()) {
            System.out.println(new String(key));
            System.out.println(new String(map.get(key)));
            System.out.println("--------------------------------------");
        }

    }
}

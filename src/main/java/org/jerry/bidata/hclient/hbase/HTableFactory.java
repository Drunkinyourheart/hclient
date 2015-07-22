package org.jerry.bidata.hclient.hbase;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * Creates clients to access HBase tables.
 *
 */
public interface HTableFactory {
    /**
     * Creates an HBase client using an externally managed HConnection and Thread pool.
     *
     */
    HTableInterface getTable(byte[] tableName, HConnection connection) throws IOException;

    /**
     * Default implementation.  Uses standard HBase HTables.
     */
    static class HTableFactoryImpl implements HTableFactory {
        @Override
        public HTableInterface getTable(byte[] tableName, HConnection connection) throws IOException {
            return new HTable(tableName, connection);
        }
    }
}

package org.jerry.bidata.hclient.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public interface HConnectionFactory {

    /**
     * Creates HConnection to access HBase clusters.
     * 
     * @param conf object
     * @return A HConnection instance
     */
    HConnection createConnection(Configuration conf) throws ZooKeeperConnectionException;

    /**
     * Default implementation.  Uses standard HBase HConnections.
     */
    static class HConnectionFactoryImpl implements HConnectionFactory {
        @Override
        public HConnection createConnection(Configuration conf) throws ZooKeeperConnectionException {
            return HConnectionManager.createConnection(conf);
        }
    }
}

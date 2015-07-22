package org.jerry.bidata.hclient.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * HBase framework-style client
 * <p/>
 * Jerry
 * since 0.1
 */
public class HClientFactory {

    private static final int DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);
    private static final byte[] LOCAL_ADDRESS = getLocalAddress();
    private static final long DEFAULT_INACTIVE_THRESHOLD_MS = (int) TimeUnit.MINUTES.toMillis(3);
    private static final int DEFAULT_CLOSE_WAIT_MS = (int) TimeUnit.SECONDS.toMillis(1);

    /**
     * Return a new builder that builds a CuratorFramework
     *
     * @return new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
        private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
        private int maxCloseWaitMs = DEFAULT_CLOSE_WAIT_MS;
        private String zkConnectString;
        private Configuration configuration;
        private HConnection connection;
        private byte[] tableName;

        public Builder() {
            if (configuration == null) {
                this.configuration = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
                this.configuration = HBaseConfiguration.create(configuration);
            }
            if (connection == null) {
                try {
                    this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.configuration);
                } catch (ZooKeeperConnectionException e) {
                    throw new RuntimeException("can't connect zk.");
                }
                if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
                    throw new RuntimeException("Unable to establish connection.");
                }
            }
        }

        /**
         * Set the list of servers to connect to. IMPORTANT: use either this or {@link # ensembleProvider(EnsembleProvider)}
         * but not both.
         *
         * @param connectString list of servers to connect to
         * @return this
         */
        public Builder connectString(String connectString) {
            if (connectString == null) {
                throw new RuntimeException("connectString is null.");
            }
            this.zkConnectString = connectString;
            // conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
            configuration.set("hbase.zookeeper.quorum", connectString);
            return this;
        }

        public Builder configuration(Configuration config) {
            this.configuration = config;
            return this;
        }

        public Builder hConnection(HConnection hConnection) {
            this.connection = hConnection;
            return this;
        }

        public Builder table(byte[] tableName) {
            this.tableName = tableName;
            return this;
        }


        public HClient build() throws TableNotFoundException {

            HTableInterface hTableInterface = null;
            try {
                hTableInterface = HBaseFactoryProvider.getHTableFactory().getTable(tableName, connection);
                return createHClient(hTableInterface);
            } catch (TableNotFoundException e) {
                throw new TableNotFoundException(Bytes.toString(tableName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        private HClient createHClient(HTableInterface hTableInterface) throws IOException {
            HClient hClient = new HClient(hTableInterface);
            return hClient;
        }

    }


    public static HClient create() throws IOException {

        return new HClient(null);
    }

    private Configuration config;
    private HConnection connection;

    public HTableInterface getTable(byte[] tableName) throws TableNotFoundException {

        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config = HBaseConfiguration.create(config);

        try {
            this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(config);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException("can't connect zk.");
        }
        if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new RuntimeException("Unable to establish connection.");
        }

        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableName, connection);
        } catch (TableNotFoundException e) {
            byte[][] schemaAndTableName = new byte[2][];
            throw new TableNotFoundException(Bytes.toString(tableName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress().getBytes();
        } catch (UnknownHostException ignore) {
            // ignore
        }
        return new byte[0];
    }

}

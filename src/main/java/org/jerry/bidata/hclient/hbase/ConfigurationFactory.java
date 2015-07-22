package org.jerry.bidata.hclient.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public interface ConfigurationFactory {

    /**
     * @return Configuration containing HBase/Hadoop settings
     */
    Configuration getConfiguration();

    /**
     * Default implementation uses {@link org.apache.hadoop.hbase.HBaseConfiguration#create()}.
     */
    static class ConfigurationFactoryImpl implements ConfigurationFactory {
        @Override
        public Configuration getConfiguration() {
            return HBaseConfiguration.create();
        }
    }
}

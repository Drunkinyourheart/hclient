package org.jerry.bidata.hclient.hbase;

import org.jerry.bidata.hclient.utils.InstanceResolver;

public class HBaseFactoryProvider {

    private static final HTableFactory DEFAULT_HTABLE_FACTORY = new HTableFactory.HTableFactoryImpl();
    private static final HConnectionFactory DEFAULT_HCONNECTION_FACTORY =
            new HConnectionFactory.HConnectionFactoryImpl();
    private static final ConfigurationFactory DEFAULT_CONFIGURATION_FACTORY = new ConfigurationFactory.ConfigurationFactoryImpl();

    public static HTableFactory getHTableFactory() {
        return InstanceResolver.getSingleton(HTableFactory.class, DEFAULT_HTABLE_FACTORY);
    }

    public static HConnectionFactory getHConnectionFactory() {
        return InstanceResolver.getSingleton(HConnectionFactory.class, DEFAULT_HCONNECTION_FACTORY);
    }

    public static ConfigurationFactory getConfigurationFactory() {
        return InstanceResolver.getSingleton(ConfigurationFactory.class, DEFAULT_CONFIGURATION_FACTORY);
    }
}

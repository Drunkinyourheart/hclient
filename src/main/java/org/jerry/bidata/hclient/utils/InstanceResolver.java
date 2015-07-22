package org.jerry.bidata.hclient.utils;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class InstanceResolver {
    private static final ConcurrentHashMap<Class, Object> RESOLVED_SINGLETONS = new ConcurrentHashMap<Class, Object>();

    private InstanceResolver() {/* not allowed */}

    @SuppressWarnings("unchecked")
    public static <T> T getSingleton(Class<T> clazz, T defaultInstance) {
        Object obj = RESOLVED_SINGLETONS.get(clazz);
        if(obj != null) {
            return (T)obj;
        }
        if (defaultInstance != null && !clazz.isInstance(defaultInstance)) throw new IllegalArgumentException("defaultInstance is not of type " + clazz.getName());
        final Object o = resolveSingleton(clazz, defaultInstance);
        obj = RESOLVED_SINGLETONS.putIfAbsent(clazz, o);
        if(obj == null) {
            obj = o;
        }
        return (T)obj;
    }
    
    private synchronized static <T> T resolveSingleton(Class<T> clazz, T defaultInstance) {
        ServiceLoader<T> loader = ServiceLoader.load(clazz);
        // returns the first registered instance found
        for (T singleton : loader) {
            return singleton;
        }
        return defaultInstance;
    }
}

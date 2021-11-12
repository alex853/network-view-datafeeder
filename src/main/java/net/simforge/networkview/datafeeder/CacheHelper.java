package net.simforge.networkview.datafeeder;

import org.ehcache.Cache;

import java.lang.reflect.Field;
import java.util.Map;

public class CacheHelper {
    public static String getEstimatedCacheSize(Cache cache) {
        try {
            Field storeField = cache.getClass().getDeclaredField("store");
            storeField.setAccessible(true);
            Object store = storeField.get(cache);
            Field mapField = store.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            Object map = mapField.get(store);
            Field realMapField = map.getClass().getDeclaredField("realMap");
            realMapField.setAccessible(true);
            Object realMap = realMapField.get(map);
            Map _map = (Map) realMap;
            return Integer.toString(_map.size());
        } catch (Exception e) {
            return "?";
        }

/*        BM.start("Archive.getEstimatedCacheSize");
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("javax.cache:type=CacheStatistics,CacheManager=" + cacheManager.getURI().toString().replaceAll(",|:|=|\n", ".") + ",Cache=" + cache.getName());
            CacheStatisticsMXBean cacheStatisticsMXBean = JMX.newMBeanProxy(mBeanServer, objectName, CacheStatisticsMXBean.class);
            return cacheStatisticsMXBean.getCachePuts() - cacheStatisticsMXBean.getCacheEvictions() - cacheStatisticsMXBean.getCacheRemovals();
        } catch (MalformedObjectNameException e) {
            logger.error("Unable to estimate cache size", e);
            throw new RuntimeException("Unable to estimate cache size", e);
        } finally {
            BM.stop();
        }*/
    }
}

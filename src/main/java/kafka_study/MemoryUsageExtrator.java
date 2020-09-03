package kafka_study;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * @author bystander
 * @date 2020/2/23
 */
public class MemoryUsageExtrator {

    private static OperatingSystemMXBean mxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     * @return  free RAM size
     */
//    public static long currentFreeMemorySizeInBytes() {
//        return mxBean.getFreePhysicalMemorySize();
//    }

}

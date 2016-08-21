package publisher.schedular.util;

/**
 * Created by sajith on 8/3/16.
 */
public class SwitchingConfigurations {
    /**
     * Threshold throughput in TPS
     */
    private static double thresholdThroughput;

    /**
     * Threshold latency for in milliseconds
     */
    private static double getThresholdLatency;

    /**
     * IP and port of the public cloud server which receives event.
     */
    private static Endpoint publicCloudEndpoint;

    /**
     * System will keep monitoring if the latency is below the threshold for tolerancePeriod. Once duration exceeds
     * tolerancePeriod it will start the VM
     */
    private static long tolerancePeriod;

    /**
     * Time taken for the VM to start
     */
    private static long vmStartDelay;

    /**
     * How long the before starting the next billing period of VM.
     */
    private static long vmBillingSessionDuration;

    /**
     * The threshold latency to start publishing data to private cloud.
     */
    private static long publicCloudPublishThresholdLatency;

    /**
     * Minimum events that should be sent public cloud in order to keep VM without shutting down
     */
    private static long minEventsToKeepVm;

    public static long getVmBillingSessionDuration() {
        return vmBillingSessionDuration;
    }

    public static void setVmBillingSessionDuration(long vmBillingSessionDuration) {
        SwitchingConfigurations.vmBillingSessionDuration = vmBillingSessionDuration;
    }

    public static long getVmStartDelay() {
        return vmStartDelay;
    }

    public static void setVmStartDelay(long vmStartDelay) {
        SwitchingConfigurations.vmStartDelay = vmStartDelay;
    }


    public static long getTolerancePeriod() {
        return tolerancePeriod;
    }

    public static void setTolerancePeriod(long tolerancePeriod) {
        SwitchingConfigurations.tolerancePeriod = tolerancePeriod;
    }

    public static double getThresholdThroughput() {
        return thresholdThroughput;
    }

    public static void setThresholdThroughput(double thresholdThroughput) {
        SwitchingConfigurations.thresholdThroughput = thresholdThroughput;
    }

    public static double getGetThresholdLatency() {
        return getThresholdLatency;
    }

    public static void setGetThresholdLatency(double getThresholdLatency) {
        SwitchingConfigurations.getThresholdLatency = getThresholdLatency;
    }

    public static Endpoint getPublicCloudEndpoint() {
        return publicCloudEndpoint;
    }

    public static void setPublicCloudEndpoint(String ipAddress, int port) {
        SwitchingConfigurations.publicCloudEndpoint = new Endpoint(ipAddress, port);
    }

    public static long getPublicCloudPublishThresholdLatency() {
        return publicCloudPublishThresholdLatency;
    }

    public static void setPublicCloudPublishThresholdLatency(long publicCloudPublishThresholdLatency) {
        SwitchingConfigurations.publicCloudPublishThresholdLatency = publicCloudPublishThresholdLatency;
    }

    public static long getMinEventsToKeepVm() {
        return minEventsToKeepVm;
    }

    public static void setMinEventsToKeepVm(long minEventsToKeepVm) {
        SwitchingConfigurations.minEventsToKeepVm = minEventsToKeepVm;
    }
}

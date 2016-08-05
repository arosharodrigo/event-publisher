package publisher.schedular;

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
}

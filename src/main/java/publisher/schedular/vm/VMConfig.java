package publisher.schedular.vm;

/**
 * Created by sajith on 11/26/16.
 */
public class VMConfig {
    private int vmId;
    private int port;
    private String ip;
    private long startThresholdLatency;
    private long tolerancePeriod;
    private long dataPublishThresholdLatency;


    public VMConfig(int vmId, int port, String ip, long startThresholdLatency, long dataPublishLatency, long tolerancePeriod) {
        this.vmId = vmId;
        this.port = port;
        this.ip = ip;
        this.startThresholdLatency = startThresholdLatency;
        this.dataPublishThresholdLatency = dataPublishLatency;
        this.tolerancePeriod = tolerancePeriod;
    }


    public String getThriftUrl(){
        return "tcp://" + ip + ":" + port;
    }

    public long getStartThresholdLatency() {
        return startThresholdLatency;
    }

    public long getTolerancePeriod() {
        return tolerancePeriod;
    }

    public long getDataPublishThresholdLatency() {
        return dataPublishThresholdLatency;
    }

    public int getVmId(){
        return vmId;
    }
}

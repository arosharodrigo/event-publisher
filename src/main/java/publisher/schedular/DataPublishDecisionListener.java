package publisher.schedular;

/**
 * Created by sajith on 11/27/16.
 */
public interface DataPublishDecisionListener {
    void SendDataToVm(int vmId);

    void StopSendingDataToVm(int vmId);
}

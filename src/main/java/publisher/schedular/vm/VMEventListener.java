package publisher.schedular.vm;

/**
 * Created by sajith on 11/26/16.
 */
public interface VMEventListener {
    void OnVMStarted(int id);

    void OnVMBillingPeriodEnding(int id);
}

package publisher.schedular.util;

/**
 * Created by sajith on 8/3/16.
 */
public class Configurations {

    /**
     * Time taken for the VM to start
     */
    private static long vmStartDelay;

    /**
     * How long the before starting the next billing period of VM.
     */
    private static long vmBillingSessionDuration;


    /**
     * Minimum events that should be sent public cloud in order to keep VM without shutting down
     */
    private static long minEventsToKeepVm;



    public static long getVmBillingSessionDuration() {
        return vmBillingSessionDuration;
    }

    public static void setVmBillingSessionDuration(long vmBillingSessionDuration) {
        Configurations.vmBillingSessionDuration = vmBillingSessionDuration;
    }

    public static long getVmStartDelay() {
        return vmStartDelay;
    }

    public static void setVmStartDelay(long vmStartDelay) {
        Configurations.vmStartDelay = vmStartDelay;
    }

    public static long getMinEventsToKeepVm() {
        return minEventsToKeepVm;
    }

    public static void setMinEventsToKeepVm(long minEventsToKeepVm) {
        Configurations.minEventsToKeepVm = minEventsToKeepVm;
    }
}

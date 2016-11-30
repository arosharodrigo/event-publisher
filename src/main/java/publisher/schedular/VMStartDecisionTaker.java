package publisher.schedular;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.util.StatisticsListener;
import publisher.schedular.vm.VMStartDecisionListener;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by sajith on 8/3/16.
 */
public class VMStartDecisionTaker implements Runnable, StatisticsListener {
    private static final long POLLING_INTERVAL = 3000l;
    private static final long EVALUATE_INTERVAL = 6000l;
    private static final long GRACE_PERIOD = 1 * 1000l;

    private static Log log = LogFactory.getLog(VMStartDecisionTaker.class);

    private  AtomicDouble currentLatency = new AtomicDouble(0l);
    private long currentElapsedTime = 0;
    private TolerancePeriodTask tolerancePeriodTask = null;
    Timer inputReaderTaskTimer = new Timer();
    Timer toleranceTimer = new Timer();
    private boolean stopped = false;
    StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask(this);

    long thresholdLatency  = 0;
    private int vmId =  -1;
    private long tolerancePeriod;
    VMStartDecisionListener listener;

    public VMStartDecisionTaker(long thresholdLatency, int vmId, long tolerancePeriod, VMStartDecisionListener listener) {
        this.thresholdLatency = thresholdLatency;
        this.vmId = vmId;
        this.tolerancePeriod = tolerancePeriod;
        this.listener = listener;
    }

    public synchronized void start(){
        stopped = false;
    }

    public synchronized void stop(){
        stopped = true;
        tolerancePeriodTask.cancel();
        tolerancePeriodTask = null;
    }

    private boolean evaluate(){
        Double latency = currentLatency.get();
        if (latency > thresholdLatency){
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        inputReaderTaskTimer.schedule(inputReaderTask, GRACE_PERIOD, POLLING_INTERVAL);
        try {
            while (true){
                if (stopped){
                    Thread.sleep(EVALUATE_INTERVAL);
                    continue;
                }

                if (evaluate()){
                    if (tolerancePeriodTask == null){
                        tolerancePeriodTask = new TolerancePeriodTask();
                        toleranceTimer.schedule(tolerancePeriodTask, tolerancePeriod);
                        log.info("Starting the tolerance timer[VM=" + vmId + ", ElapsedTime=" + currentElapsedTime + ", TolerancePeriod=" + tolerancePeriod + "]");
                    } else {
                        //System.out.println("Tolerance timer is already running");
                    }
                } else {
                    if (tolerancePeriodTask != null){
                        tolerancePeriodTask.cancel();
                        tolerancePeriodTask = null;
                        log.info("Latency has recovered with in tolerance time period, stopping tolerance timer[VM=" + vmId + ", ElapsedTime=" + currentElapsedTime + "]");
                    }
                }
                Thread.sleep(EVALUATE_INTERVAL);
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void onStatisticsRead(long elapsedTime, double latency, double throughput) {
        currentLatency.set(latency);
        currentElapsedTime = elapsedTime;
    }

    class TolerancePeriodTask extends TimerTask{
        @Override
        public void run() {
            log.info("Tolerance period is over. starting Virtual Machine[VM=" + vmId + ", ElapsedTime=" + currentElapsedTime + "]");
            tolerancePeriodTask.cancel();
            listener.onTriggerVMStartUp(vmId);
        }
    }


}

package publisher.schedular;

import com.google.common.util.concurrent.AtomicDouble;
import publisher.ResearchEventPublisher;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.util.StatisticsListener;
import publisher.schedular.util.SwitchingConfigurations;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by sajith on 8/3/16.
 */
public class VMStartDecisionTaker implements Runnable, StatisticsListener {
    private static final long POLLING_INTERVAL = 5000l;
    private static final long EVALUATE_INTERVAL = 6000l;
    private static final long GRACE_PERIOD = 60 * 1000l;

    private  AtomicDouble currentLatency = new AtomicDouble(0l);
    private long currentElapsedTime = 0;
    private TolerancePeriodTask tolerancePeriodTask = null;
    Timer inputReaderTaskTimer = new Timer();
    Timer toleranceTimer = new Timer();
    private boolean stopped = false;
    StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask(this);

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
        if (latency > SwitchingConfigurations.getVmStartTriggerThresholdLatency()){
            /*System.out.println("{" + new Date().toString() + ":" + currentElapsedTime + "} - Latency is greater than threshold[Current Latency :" + latency
                    + ", Threshold latency " + SwitchingConfigurations.getVmStartTriggerThresholdLatency() +" ]");*/
            return true;
        } else {
            /*System.out.println("{" + new Date().toString() + ":" + currentElapsedTime + "} - Latency is less than threshold[Current Latency :" + latency
                    + ", Threshold latency " + SwitchingConfigurations.getVmStartTriggerThresholdLatency() +" ]");*/
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
                        toleranceTimer.schedule(tolerancePeriodTask,  SwitchingConfigurations.getTolerancePeriod());
                        System.out.println("{" + new Date().toString() + ":" + currentElapsedTime + "}[EVENT] - Starting the tolerance timer");
                    } else {
                        //System.out.println("Tolerance timer is already running");
                    }
                } else {
                    if (tolerancePeriodTask != null){
                        tolerancePeriodTask.cancel();
                        tolerancePeriodTask = null;
                        System.out.println("{" + new Date().toString() + ":" + currentElapsedTime +
                                "}[EVENT] - Latency has recovered with in tolerance time period, stopping tolerance timer at ");
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
            System.out.println("{" + new Date().toString() + ":" + currentElapsedTime + "}[EVENT] - Tolerance period is over. starting Virtual Machine");
            tolerancePeriodTask.cancel();
            ResearchEventPublisher.StartVM();
        }
    }


}

package publisher.schedular;

import com.google.common.util.concurrent.AtomicDouble;
import com.hazelcast.nio.Data;
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
    private static final long POLLING_INTERVAL = 10000l;
    private static final long EVALUATE_INTERVAL = 11000l;
    private static final long GRACE_PERIOD = 60 * 1000l;

    private  AtomicDouble currentLatency = new AtomicDouble(0l);
    private TolerancePeriodTimer tolerancePeriodTimerTask = null;
    Timer inputReaderTaskTimer = new Timer();
    Timer toleranceTimer = new Timer();

    private boolean evaluate(){
        Double latency = currentLatency.get();
        if (latency > SwitchingConfigurations.getGetThresholdLatency()){
            System.out.println("Latency is less than threshold[Current Latency :" + latency
                    + ", Threshold latency " + SwitchingConfigurations.getGetThresholdLatency() +" ]");
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask(this);

        inputReaderTaskTimer.schedule(inputReaderTask, GRACE_PERIOD, POLLING_INTERVAL);
        try {
            while (true){
                if (evaluate()){
                    if (tolerancePeriodTimerTask == null){
                        tolerancePeriodTimerTask = new TolerancePeriodTimer();
                        toleranceTimer.schedule(tolerancePeriodTimerTask,  SwitchingConfigurations.getTolerancePeriod());
                        System.out.println("Starting the tolerance timer at " + new Date().toString());
                    } else {
                        System.out.println("Tolerance timer is already running");
                    }
                } else {
                    if (tolerancePeriodTimerTask != null){
                        tolerancePeriodTimerTask.cancel();
                        tolerancePeriodTimerTask = null;
                        System.out.println("Latency has recovered with in tolerance time period, stopping tolerance timer at " + new Data().toString());
                    }
                }
                Thread.sleep(EVALUATE_INTERVAL);
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void onStatisticsRead(double latency, double throughput) {
        currentLatency.set(latency);
        System.out.println("Setting Latency...... " +  latency);
    }

    class TolerancePeriodTimer extends TimerTask{
        @Override
        public void run() {
            System.out.println("Tolerance period is over, starting VM at " + new Date().toString());
            inputReaderTaskTimer.cancel();
            toleranceTimer.cancel();
            ResearchEventPublisher.StartVM();
        }
    }


}

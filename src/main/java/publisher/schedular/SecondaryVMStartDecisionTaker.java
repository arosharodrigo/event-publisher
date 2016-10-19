package publisher.schedular;

import com.google.common.util.concurrent.AtomicDouble;
import publisher.ResearchEventPublisher;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.util.StatisticsListener;
import publisher.schedular.util.SwitchingConfigurations;

import java.util.Date;
import java.util.Timer;

/**
 * Created by sajith on 10/10/16.
 */
public class SecondaryVMStartDecisionTaker implements Runnable, StatisticsListener {
    private static final long POLLING_INTERVAL = 1000l;
    private static final long EVALUATE_INTERVAL = 1500l;
    private static final long GRACE_PERIOD = 5 * 1000l;

    StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask(this);
    Timer inputReaderTaskTimer = new Timer();
    private boolean stopped = false;
    private AtomicDouble currentLatency = new AtomicDouble(0l);
    private long currentElapsedTime = 0;
    private int consecutiveThresholdExceedCount = 0;

    public synchronized void start(){
        stopped = false;
    }

    public synchronized void stop(){
        stopped = true;
    }

    public boolean evaluvate(){
        if (currentLatency.get() > SwitchingConfigurations.getSecondaryVmStartupThreshold()){
            consecutiveThresholdExceedCount++;
        }

        if (consecutiveThresholdExceedCount >= SwitchingConfigurations.getSecondaryVmStartupThresholdConsecutiveCount()){
            consecutiveThresholdExceedCount = 0;
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

                if (evaluvate()){
                    System.out.println("{" + new Date().toString() + ":" + currentElapsedTime + "}[EVENT] - starting Secondary Virtual Machine");
                    ResearchEventPublisher.StartVM(ResearchEventPublisher.VM_ID_SECOND);
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
}

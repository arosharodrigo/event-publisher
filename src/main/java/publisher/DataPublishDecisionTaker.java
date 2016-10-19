package publisher;

import com.google.common.util.concurrent.AtomicDouble;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.util.StatisticsListener;
import publisher.schedular.util.SwitchingConfigurations;

import java.util.Date;
import java.util.Timer;

/**
 * Created by sajith on 8/21/16.
 */
public class DataPublishDecisionTaker implements StatisticsListener, Runnable{
    private static final long POLLING_INTERVAL = 10000l;
    private static final long EVALUATE_INTERVAL = 11000l;
    private static final long GRACE_PERIOD = 5 * 1000l;


    Timer inputReaderTaskTimer = new Timer();
    private AtomicDouble currentLatency = new AtomicDouble(0l);
    private long currentElapsedTime = 0l;
    private boolean stopped = true;

    public synchronized void start(){
        stopped = false;
    }

    public synchronized void stop(){
        stopped = true;
    }

    public double getCurrentLatency(){
        return currentLatency.get();
    }

    @Override
    public void onStatisticsRead(long elapsedTime, double latency, double throughput) {
        currentElapsedTime = elapsedTime;
        currentLatency.set(latency);
    }

    @Override
    public void run() {
        StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask(this);
        inputReaderTaskTimer.schedule(inputReaderTask, GRACE_PERIOD, POLLING_INTERVAL);
        try {
            while (true) {
                if (stopped){
                    Thread.sleep(EVALUATE_INTERVAL);
                    continue;
                }

                if (shouldPublishToPublicClould()) {
                    ResearchEventPublisher.StartSendingToPublicCloud();
                } else {
                    ResearchEventPublisher.StopSendingToPublicCloud();
                }

                if (shouldSendToSecondaryInstance()){
                    ResearchEventPublisher.SendToSecondaryInstance();
                } else {
                    ResearchEventPublisher.StopSendingToSecondaryInstance();
                }
                Thread.sleep(EVALUATE_INTERVAL);
            }
        } catch (Exception e){

        }
    }

    public boolean shouldPublishToPublicClould(){
        double latency  = currentLatency.get();
        if (latency > SwitchingConfigurations.getPublicCloudPublishThresholdLatency()){
            System.out.println("{ "+ new Date().toString() + ":" + currentElapsedTime + "}  - Latency greater than threshold latency. Send data to PUBLIC cloud [Current Latency:"
                    + latency + " , Threshold Latency : " + SwitchingConfigurations.getPublicCloudPublishThresholdLatency() + "]");
            return true;
        } else {
            System.out.println("{ "+ new Date().toString() + ":" + currentElapsedTime + "}  - Latency less than threshold latency. Send data to PRIVATE cloud [Current Latency:"
                    + latency + " , Threshold Latency : " + SwitchingConfigurations.getPublicCloudPublishThresholdLatency() + "]");
            return false;
        }
    }

    public boolean shouldSendToSecondaryInstance(){
        double latency = currentLatency.get();
        if (latency > SwitchingConfigurations.getSecondaryVmDataPublishThreshold()){
            System.out.println("{ "+ new Date().toString() + ":" + currentElapsedTime + "}  - Latency greater than threshold latency. Send data to PUBLIC Secondary Instance[Current Latency:"
                    + latency + " , Threshold Latency : " + SwitchingConfigurations.getSecondaryVmDataPublishThreshold() + "]");
            return true;

        }else {
            System.out.println("{ "+ new Date().toString() + ":" + currentElapsedTime + "}  - Latency less than threshold latency. Don't Send data to Public Secondary Instance [Current Latency:"
                    + latency + " , Threshold Latency : " + SwitchingConfigurations.getSecondaryVmDataPublishThreshold() + "]");
            return false;
        }
    }
}

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
    private static final long GRACE_PERIOD = 60 * 1000l;


    Timer inputReaderTaskTimer = new Timer();
    private AtomicDouble currentLatency = new AtomicDouble(0l);

    @Override
    public void onStatisticsRead(double latency, double throughput) {
        currentLatency.set(latency);
    }

    @Override
    public void run() {
        StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask(this);
        inputReaderTaskTimer.schedule(inputReaderTask, GRACE_PERIOD, POLLING_INTERVAL);
        try {
            while (true) {
                if (shouldPublishToPublicClould()) {
                    ResearchEventPublisher.StartSendingToPublicCloud();
                } else {
                    ResearchEventPublisher.StopSendingToPublicCloud();
                }
                Thread.sleep(EVALUATE_INTERVAL);
            }
        } catch (Exception e){

        }
    }

    public boolean shouldPublishToPublicClould(){
        double latecny  = currentLatency.get();
        if (latecny > SwitchingConfigurations.getPublicCloudPublishThresholdLatency()){
            System.out.println("{ "+ new Date().toString() + "}  - Latency greater than threshold latency. Send data to PUBLIC cloud [Current Latency:"
                    + latecny + " , Threshold Latency : " + SwitchingConfigurations.getPublicCloudPublishThresholdLatency() + "]");
            return true;
        } else {
            System.out.println("{ "+ new Date().toString() + "}  - Latency less than threshold latency. Send data to PRIVATE cloud [Current Latency:"
                    + latecny + " , Threshold Latency : " + SwitchingConfigurations.getPublicCloudPublishThresholdLatency() + "]");
            return false;
        }
    }
}

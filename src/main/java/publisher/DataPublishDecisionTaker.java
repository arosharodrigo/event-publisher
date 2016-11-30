package publisher;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import publisher.schedular.DataPublishDecisionListener;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.util.StatisticsListener;

import java.util.Timer;

/**
 * Created by sajith on 8/21/16.
 */
public class DataPublishDecisionTaker implements StatisticsListener, Runnable{
    private static final long POLLING_INTERVAL = 10000l;
    private static final long EVALUATE_INTERVAL = 11000l;
    private static final long GRACE_PERIOD = 5 * 1000l;

    private static Log log = LogFactory.getLog(DataPublishDecisionTaker.class);
    Timer inputReaderTaskTimer = new Timer();
    private AtomicDouble currentLatency = new AtomicDouble(0l);
    private long currentElapsedTime = 0l;
    private boolean stopped = true;
    private long dataPublishThresholdLatency;
    private int vmId;
    DataPublishDecisionListener listener;

    public DataPublishDecisionTaker(int vmId, long dataPublishThresholdLatency, DataPublishDecisionListener listener){
        this.dataPublishThresholdLatency = dataPublishThresholdLatency;
        this.vmId = vmId;
        this.listener = listener;
    }

    public synchronized void start(){
        stopped = false;
    }

    public synchronized void stop(){
        stopped = true;
    }

    public boolean isLatencyRecovered(){
        return currentLatency.get() < dataPublishThresholdLatency;
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

                if (shouldPublishToPublicCloud()) {
                    listener.SendDataToVm(vmId);
                } else {
                    listener.StopSendingDataToVm(vmId);
                }

                Thread.sleep(EVALUATE_INTERVAL);
            }
        } catch (Exception e){

        }
    }

    public boolean shouldPublishToPublicCloud(){
        double latency  = currentLatency.get();
        if (latency > dataPublishThresholdLatency){
            log.info("Latency greater than threshold latency. Send data to PUBLIC cloud[ID=" + vmId + ", CurrentLatency="
                    + latency + ", ThresholdLatency=" + dataPublishThresholdLatency + ", ElapsedTime=" + currentElapsedTime +"]");
            return true;
        } else {
            log.debug("Latency less than threshold latency. Send data to PRIVATE cloud[ID=" + vmId + ", CurrentLatency="
                    + latency + ", ThresholdLatency=" + dataPublishThresholdLatency + ", ElapsedTime=" + currentElapsedTime +"]");
            return false;
        }
    }
}

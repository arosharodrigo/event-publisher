package publisher.schedular;

import com.google.common.util.concurrent.AtomicDouble;
import publisher.ResearchEventPublisher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by sajith on 8/3/16.
 */
public class DecisionTaker implements Runnable {
    private static final long POLLING_INTERVAL = 30000l;
    private static final long EVALUATE_INTERVAL = 4000l;
    private static final long GRACE_PERIOD = 60 * 1000l;

    private  AtomicDouble currentThroughput = new AtomicDouble(0l);
    private  AtomicDouble currentLatency = new AtomicDouble(0l);


    private boolean evaluate(){
        if (currentLatency.get() > SwitchingConfigurations.getGetThresholdLatency() ||
                currentThroughput.get() < SwitchingConfigurations.getThresholdThroughput()){
            System.out.println("Need to switch.....");
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        StatisticsInputReaderTask inputReaderTask = new StatisticsInputReaderTask();
        Timer inputReaderTaskTimer = new Timer();
        inputReaderTaskTimer.schedule(inputReaderTask, GRACE_PERIOD, POLLING_INTERVAL);
        try {
            Thread.sleep(GRACE_PERIOD + 1000);
            while (true){
                if (evaluate()){
                    ResearchEventPublisher.Switch();
                }
                Thread.sleep(EVALUATE_INTERVAL);
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


     class StatisticsInputReaderTask extends TimerTask{

         private static final int CURRENT_THROUGHPUT_COLUMN = 3;
         private static final int CURRENT_LATENCY_COLUMN = 4;

         public void readCurrentValuesFromFile() {
         try {
             System.out.println("Reading" + ".......");
                BufferedReader input = new BufferedReader(new FileReader("/home/sajith/research/statistics-collector/results.csv"));
                String lastLine = null;
                String currentline;

                while ((currentline = input.readLine()) != null) {
                    lastLine = currentline;
                }

                if (lastLine != null) {
                    String[] values = lastLine.split(",");

                    currentThroughput.set(Double.parseDouble(values[CURRENT_THROUGHPUT_COLUMN]));
                    currentLatency.set(Double.parseDouble(values[CURRENT_LATENCY_COLUMN]));
                    System.out.println("Setting Current Values. Throughput : " + currentThroughput.get() + ", Latency : " + currentLatency.get());
                }
                input.close();
             } catch (Exception e) {
                System.out.println("Error while reading values from file" + e.getMessage());
                e.printStackTrace();
            }

         }

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
           readCurrentValuesFromFile();
            // Read the file and set the value
        }
    }
}

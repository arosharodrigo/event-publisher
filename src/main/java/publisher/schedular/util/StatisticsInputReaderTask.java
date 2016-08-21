package publisher.schedular.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.TimerTask;

/**
 * Created by sajith on 8/17/16.
 */
public class StatisticsInputReaderTask extends TimerTask {

    StatisticsListener statisticsListener;

    public StatisticsInputReaderTask(StatisticsListener statisticsListener) {
        this.statisticsListener = statisticsListener;
    }

    private static final int CURRENT_THROUGHPUT_COLUMN = 3;
    private static final int CURRENT_LATENCY_COLUMN = 4;

    public void readCurrentValuesFromFile() {
        try {
            BufferedReader input = new BufferedReader(new FileReader("/home/sajith/research/statistics-collector/results.csv"));
            String lastLine = null;
            String currentLine;

            while ((currentLine = input.readLine()) != null) {
                lastLine = currentLine;
            }

            if (lastLine != null) {
                String[] values = lastLine.split(",");
                double latency = Double.parseDouble(values[CURRENT_LATENCY_COLUMN]);
                double throughput = Double.parseDouble(values[CURRENT_THROUGHPUT_COLUMN]);
                statisticsListener.onStatisticsRead(latency, throughput);
                System.out.println("Reading statistics. Throughput : " + throughput + ", Latency : " + latency);
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

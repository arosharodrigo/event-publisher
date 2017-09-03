package publisher.schedular.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
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
    private static final int ELAPSED_TIME_COLUMN = 0;

    private static Properties prop;

    static {
        prop = new Properties();
        InputStream input = null;
        try {
            String filename = "config.properties";
            input = StatisticsInputReaderTask.class.getClassLoader().getResourceAsStream(filename);
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void readCurrentValuesFromFile() {
        try {
            BufferedReader input = new BufferedReader(new FileReader(prop.getProperty("result.file.fullpath")));
            String lastLine = null;
            String currentLine;

            while ((currentLine = input.readLine()) != null) {
                lastLine = currentLine;
            }

            if (lastLine != null) {
                String[] values = lastLine.split(",");
                long elapsedTime = values[ELAPSED_TIME_COLUMN].equals("0.0")? 0L : Long.parseLong(values[ELAPSED_TIME_COLUMN]);
                double latency = Double.parseDouble(values[CURRENT_LATENCY_COLUMN]);
                double throughput = Double.parseDouble(values[CURRENT_THROUGHPUT_COLUMN]);
                statisticsListener.onStatisticsRead(elapsedTime, latency, throughput);
                //System.out.println("Reading statistics. Throughput : " + throughput + ", Latency : " + latency);
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

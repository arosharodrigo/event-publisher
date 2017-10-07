package publisher.util;

import publisher.filter.AsyncCompositeHeEventPublisher;
import publisher.schedular.util.StatisticsInputReaderTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

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
        AsyncCompositeHeEventPublisher.init();
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

}

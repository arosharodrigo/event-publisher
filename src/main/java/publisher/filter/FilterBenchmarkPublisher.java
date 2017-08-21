package publisher.filter;

import publisher.Publishable;
import publisher.ResearchEventPublisher;

import java.util.Random;

/**
 * Created by arosha on 7/24/17.
 */
public class FilterBenchmarkPublisher extends Publishable implements Runnable {

    private String publicInputStream;

    public FilterBenchmarkPublisher(String privateInputStream, String publicInputStream) {
        super(privateInputStream, "");
        this.publicInputStream = publicInputStream;
    }

    public void publish(Object[] event) throws InterruptedException {
        ResearchEventPublisher.publishEvent(event, getStreamId());
    }

    @Override
    public void startPublishing() {
        Thread publisherThread = new Thread(this);
        publisherThread.start();
    }

    @Override
    public void run() {
        try {
            Random rand = new Random();
            Object[] dataItem = new Object[]{System.currentTimeMillis(), rand.nextLong()};
            while (true) {
                dataItem[0] = System.currentTimeMillis();
                dataItem[1] = 22L;
//                for (int i = 0; i < 1000; i ++) {
                    publish(dataItem);
//                }
                Thread.sleep(100);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public String getStreamId(boolean isPublic) {
        return isPublic ? publicInputStream : getStreamId();
    }
}

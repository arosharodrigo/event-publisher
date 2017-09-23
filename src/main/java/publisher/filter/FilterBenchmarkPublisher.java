package publisher.filter;

import publisher.Publishable;
import publisher.ResearchEventPublisher;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for(int i = 0;i < 20;i++) {
                        Object[] dataItem = new Object[]{System.currentTimeMillis(), 22L};
                        publish(dataItem);
                    }
                } catch (Throwable t) {
                    System.out.println("Error 6 - " + t);
                    t.printStackTrace();
                }
            }
        }, 2000, 10, TimeUnit.MILLISECONDS);
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

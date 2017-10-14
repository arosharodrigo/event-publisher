package publisher.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.commons.Event;
import publisher.ResearchEventPublisher;
import publisher.schedular.PublicCloudDataPublishManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncCompositeHeEventPublisher {

    private static Log log = LogFactory.getLog(AsyncCompositeHeEventPublisher.class);

    private static Queue<Event> plainQueue;
    private static Queue<Event> encryptedQueue;
    private static DataPublisher dataPublisher;
    private static ScheduledExecutorService scheduledExecutorService;
    private static ExecutorService encryptExecutorService;
    private static final int batchSize = 478;

    private static AtomicLong totalEncryptedCount = new AtomicLong(0);

    public static void init() throws Exception {
        plainQueue = new ArrayBlockingQueue<>(10000000);
        encryptedQueue = new ArrayBlockingQueue<>(10000000);

        encryptExecutorService = Executors.newSingleThreadExecutor();
        encryptExecutorService.submit(() -> {
            try {
                while(true) {
                    Event event = plainQueue.poll();
                    if (event != null) {
                        Event encryptedEvent = new Event(event.getStreamId(), event.getTimeStamp(), null, null, ResearchEventPublisher.encrypt2(event.getPayloadData()));
                        encryptedQueue.add(encryptedEvent);
                        totalEncryptedCount.incrementAndGet();
                    } else {
                        Thread.sleep(50);
                    }
                }
            } catch (Throwable th) {
                log.error("Error occurred in encrypting thread", th);
            }
        });

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    int encryptedQueueSize = encryptedQueue.size();
                    if(encryptedQueueSize > 0) {
//                        log.info("Current Encrypted queue size [" + encryptedQueueSize + "], Total Encrypted count [" + totalEncryptedCount.get() + "]");
                        if(dataPublisher == null) {
                            dataPublisher = PublicCloudDataPublishManager.generateDataPublisher(1);
                        }
                        for(int i=0; i < encryptedQueueSize; i++) {
                            Event event = encryptedQueue.poll();
                            dataPublisher.tryPublish(event);
                        }
                    } else {
//                        System.out.println("");
                    }
                } catch (Throwable t) {
                    log.error("Error 7 - " + t);
                }
        }, 10000, 10, TimeUnit.MILLISECONDS);

//        encryptExecutorService = Executors.newFixedThreadPool(100);
//        encryptExecutorService.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                int currentQueueSize = queue.size();
//                int iterations = currentQueueSize > 1000 ? 1000 : currentQueueSize;
//                for(int i=0; i<iterations; i++) {
//                    Event event = queue.poll();
//                    Event encryptedEvent = new Event(event.getStreamId(), System.currentTimeMillis(), null, null, ResearchEventPublisher.encrypt2(event.getPayloadData()));
//                    encryptedQueue.add(encryptedEvent);
//                }
//            }
//        }, 10000, 10, TimeUnit.MILLISECONDS);

    }

    public static void addToQueue(Event event) {
        plainQueue.add(event);
    }

}

package publisher.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.Event;
import publisher.ResearchEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncCompositeHeEventPublisher {

    private static Log log = LogFactory.getLog(AsyncCompositeHeEventPublisher.class);

    private static final String FIELD_SEPARATOR = "###";

    private static Queue<Event> plainQueue;
    private static Queue<Event> encryptedQueue;

    private static ExecutorService encryptExecutorService;
    private static ExecutorService encryptScheduler;
    private static ScheduledExecutorService encryptedEventsPublishExecutorService;

    private static ScheduledExecutorService logExecutorService;
    private static final int batchSize = 478;

    private static AtomicLong totalEncryptedCount = new AtomicLong(0);

    public static void init() throws Exception {
        plainQueue = new ArrayBlockingQueue<>(10000000);
        encryptedQueue = new ArrayBlockingQueue<>(10000000);

        encryptExecutorService = Executors.newFixedThreadPool(20);

        encryptScheduler = Executors.newSingleThreadExecutor();
        encryptScheduler.submit(() -> {
            try {
                while(true) {
                    if(plainQueue.size() > 10) {
                        List<Event> events = new ArrayList<>();
                        for(int i=0; i < 10; i++) {
                            events.add(plainQueue.poll());
                        }
                        encryptExecutorService.submit(() -> {
                            Event encryptedEvent = new Event(event.getStreamId(), event.getTimeStamp(), null, null, ResearchEventPublisher.encrypt2(event.getPayloadData()));
                            encryptedQueue.add(encryptedEvent);
                            totalEncryptedCount.addAndGet(10);
                        });
                    } else {
                        Thread.sleep(50);
                    }
                }
            } catch (Throwable th) {
                log.error("Error occurred in encrypting thread", th);
            }
        });

        encryptedEventsPublishExecutorService = Executors.newSingleThreadScheduledExecutor();
        encryptedEventsPublishExecutorService.scheduleAtFixedRate(() -> {
                try {
                    int encryptedQueueSize = encryptedQueue.size();
                    if(encryptedQueueSize > 0) {
//                        if(dataPublisher == null) {
//                            dataPublisher = PublicCloudDataPublishManager.generateDataPublisher(1);
//                        }
                        for(int i=0; i < encryptedQueueSize; i++) {
                            Event event = encryptedQueue.poll();
//                            dataPublisher.tryPublish(event);
                            ResearchEventPublisher.sendThroughPrivatePublisher(event);
                        }
                    } else {
//                        System.out.println("");
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
        }, 5000, 10, TimeUnit.MILLISECONDS);

        logExecutorService = Executors.newSingleThreadScheduledExecutor();
        logExecutorService.scheduleAtFixedRate(() -> {
                log.info("Plain queue size [" + plainQueue.size() + "], Encrypted queue size [" + encryptedQueue.size() + "], Total Encrypted count [" + totalEncryptedCount.get() + "]");
        }, 5000, 5000, TimeUnit.MILLISECONDS);

    }

    public static void addToQueue(Event event) {
        plainQueue.add(event);
    }

    public static Event createCompositeEvent(List<Event> events){
        StringBuilder feild1Builder = new StringBuilder();
        StringBuilder feild2Builder = new StringBuilder();
        StringBuilder feild3Builder = new StringBuilder();
        StringBuilder feild4Builder = new StringBuilder();
        StringBuilder feild5Builder = new StringBuilder();
        StringBuilder feild6Builder = new StringBuilder();
        StringBuilder feild7Builder = new StringBuilder();
        StringBuilder feild8Builder = new StringBuilder();
        for(Event event : events) {
            Object[] payloadData = event.getPayloadData();
            feild1Builder.append(payloadData[0]).append(FIELD_SEPARATOR);
            feild5Builder.append(payloadData[4]).append(FIELD_SEPARATOR);
            feild6Builder.append(payloadData[5]).append(FIELD_SEPARATOR);
            feild7Builder.append(payloadData[6]).append(FIELD_SEPARATOR);
            feild8Builder.append(payloadData[7]).append(FIELD_SEPARATOR);

            String from = (String)payloadData[1];
        }

        Object[] modifiedPayload = new Object[eventPayload.length];
        try {
            modifiedPayload[0] = eventPayload[0];
            modifiedPayload[4] = eventPayload[4];
            modifiedPayload[5] = eventPayload[5];
            modifiedPayload[6] = eventPayload[6];
            modifiedPayload[7] = eventPayload[7];

            String from = (String)eventPayload[1];
            modifiedPayload[1] = encryptToStr(from, batchSize);
            String to = (String)eventPayload[2];
            String[] toArr = to.split(",");
            modifiedPayload[2] = encryptToStr(toArr[0], batchSize);
            String cc = (String)eventPayload[3];
            String[] ccArr = cc.split(",");
            modifiedPayload[3] = encryptToStr(ccArr[0], batchSize);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error2 - " + e);
        }
        return modifiedPayload;
    }

    private static String encryptToStr(String param, int batchSize) {
        StringBuilder valueBuilder = new StringBuilder();
        byte[] paramBytes = param.getBytes();
        for(byte value : paramBytes) {
            valueBuilder.append(value);
            valueBuilder.append(",");
        }
        int dummyCount = batchSize - paramBytes.length;
        for(int i = 0;i < dummyCount; i++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueList = valueBuilder.toString().replaceAll(",$", "");
//        String encryptedParam = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(valueList);
        return valueList;
    }

}

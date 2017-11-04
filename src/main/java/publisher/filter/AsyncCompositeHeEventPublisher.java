package publisher.filter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
    private static final String COMMA_SEPARATOR = ",";

    private static Queue<Event> plainQueue;
    private static Queue<Event> encryptedQueue;

    private static ExecutorService encryptWorkers;
    private static ExecutorService encryptBossScheduler;
    private static ScheduledExecutorService encryptedEventsPublisher;

//    private static ExecutorService encryptedEventsPublishScheduler;

    private static ScheduledExecutorService eventCountPrinter;
    private static final int batchSize = 478;
    private static final int maxEmailLength = 40;
    private static final int compositeEventSize = 10;

    private static AtomicLong totalPlainCount = new AtomicLong(0);
    private static AtomicLong totalEncryptedCount = new AtomicLong(0);

    public static void init() throws Exception {
        plainQueue = new ArrayBlockingQueue<>(10000000);
        encryptedQueue = new ArrayBlockingQueue<>(10000000);

        encryptWorkers = Executors.newFixedThreadPool(20, new ThreadFactoryBuilder().setNameFormat("Composite-Event-Encode-Workers").build());

        encryptBossScheduler = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Encrypt-Boss").build());
        encryptBossScheduler.submit(() -> {
            try {
                while(true) {
                    if(plainQueue.size() > compositeEventSize) {
                        List<Event> events = new ArrayList<>();
                        for(int i=0; i < compositeEventSize; i++) {
                            events.add(plainQueue.poll());
                        }
                        encryptWorkers.submit(() -> {
                            try {
                                Event encryptedEvent = createCompositeEvent(events);
                                encryptedQueue.add(encryptedEvent);
                                totalEncryptedCount.addAndGet(compositeEventSize);
                            } catch (Exception th) {
                                log.error("Error occurred in encrypt worker thread", th);
                            }
                        });
                    } else {
                        Thread.sleep(5);
                    }
                }
            } catch (Throwable th) {
                log.error("Error occurred in encrypting thread", th);
            }
        });

        encryptedEventsPublisher = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Encrypted-Events-Publisher").build());
        encryptedEventsPublisher.scheduleAtFixedRate(() -> {
                try {
                    int encryptedQueueSize = encryptedQueue.size();
                    if(encryptedQueueSize > 0) {
                        for(int i=0; i < encryptedQueueSize; i++) {
                            Event event = encryptedQueue.poll();
                            ResearchEventPublisher.sendThroughPrivatePublisher(event);
                        }
                    } else {
                        // Nothing to do
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
        }, 5000, 10, TimeUnit.MILLISECONDS);

        /*encryptedEventsPublishScheduler = Executors.newSingleThreadExecutor();
        encryptedEventsPublishScheduler.submit(() -> {
            try {
                while(true) {
                    long startTime = System.currentTimeMillis();
                    int encryptedQueueSize = encryptedQueue.size();
                    for (int j = 0; j < encryptedQueueSize; j++) {
                        long time = System.currentTimeMillis();
                        if (time - startTime <= 8000) {
                            Event event = encryptedQueue.poll();
                            ResearchEventPublisher.sendThroughPrivatePublisher(event);
                        } else {

                        }
                    }
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - startTime <= 10000) {
                        Thread.sleep(10000 - (currentTime - startTime));
                    }
                }
            } catch (Throwable th) {
                log.error("Error occurred in encrypting thread", th);
            }
        });*/

        eventCountPrinter = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Event-Count-Printer").build());
        eventCountPrinter.scheduleAtFixedRate(() -> {
                log.info("Plain queue size [" + plainQueue.size() + "], Total Plain Count [" + totalPlainCount.get() + "], Encrypted queue size [" + encryptedQueue.size() + "], Total Encrypted count [" + totalEncryptedCount.get() + "]");
        }, 5000, 5000, TimeUnit.MILLISECONDS);

    }

    public static void addToQueue(Event event) {
        plainQueue.add(event);
        totalPlainCount.incrementAndGet();
    }

    public static Event createCompositeEvent(List<Event> events){
        StringBuilder field1Builder = new StringBuilder();
        StringBuilder field2Builder = new StringBuilder();
        StringBuilder field3Builder = new StringBuilder();
        StringBuilder field4Builder = new StringBuilder();
        StringBuilder field5Builder = new StringBuilder();
        StringBuilder field6Builder = new StringBuilder();
        StringBuilder field7Builder = new StringBuilder();
        for(Event event : events) {
            Object[] payloadData = event.getPayloadData();
            field1Builder.append(payloadData[0]).append(FIELD_SEPARATOR);
            field5Builder.append(payloadData[4]).append(FIELD_SEPARATOR);
            field6Builder.append(payloadData[5]).append(FIELD_SEPARATOR);
            field7Builder.append(payloadData[6]).append(FIELD_SEPARATOR);

            String from = (String)payloadData[1];
            field2Builder.append(convertToBinaryForm(from, maxEmailLength)).append(COMMA_SEPARATOR);

            String to = (String)payloadData[2];
            String[] toArr = to.split(",");
            field3Builder.append(convertToBinaryForm(toArr[0], maxEmailLength)).append(COMMA_SEPARATOR);

            String cc = (String)payloadData[3];
            String[] ccArr = cc.split(",");
            field4Builder.append(convertToBinaryForm(ccArr[0], maxEmailLength)).append(COMMA_SEPARATOR);
        }

        Object[] modifiedPayload = new Object[7];
        String field1Str = field1Builder.toString();
        modifiedPayload[0] = field1Str.substring(0, field1Str.length() - 3);
        String field5Str = field5Builder.toString();
        modifiedPayload[4] = field5Str.substring(0, field5Str.length() - 3);
        String field6Str = field6Builder.toString();
        modifiedPayload[5] = field6Str.substring(0, field6Str.length() - 3);
        String field7Str = field7Builder.toString();
        modifiedPayload[6] = field7Str.substring(0, field7Str.length() - 3);

        int remainingSlots = batchSize - (compositeEventSize * maxEmailLength);
        for(int i = 0; i < remainingSlots; i++) {
            field2Builder.append(0);
            field2Builder.append(",");
            field3Builder.append(0);
            field3Builder.append(",");
            field4Builder.append(0);
            field4Builder.append(",");
        }

        String field2Str = field2Builder.toString();
        String field2 = field2Str.substring(0, field2Str.length() - 1);
        String encryptedField2 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field2);
        modifiedPayload[1] = encryptedField2;

        String field3Str = field3Builder.toString();
        String field3 = field3Str.substring(0, field3Str.length() - 1);
        String encryptedField3 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field3);
        modifiedPayload[2] = encryptedField3;

        String field4Str = field4Builder.toString();
        String field4 = field4Str.substring(0, field4Str.length() - 1);
        String encryptedField4 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field4);
        modifiedPayload[3] = encryptedField4;

        return new Event(events.get(0).getStreamId(), events.get(0).getTimeStamp(), null, null, modifiedPayload);
    }

    private static String convertToBinaryForm(String param, int batchSize) {
        StringBuilder valueBuilder = new StringBuilder();
        byte[] paramBytes = param.getBytes();
        int minimumSize = (paramBytes.length < batchSize) ? paramBytes.length : batchSize;
        for(int i = 0; i < minimumSize; i++) {
            valueBuilder.append(paramBytes[i]);
            valueBuilder.append(",");
        }
        int dummyCount = batchSize - minimumSize;
        for(int j = 0;j < dummyCount; j++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueStr = valueBuilder.toString();
        String valueList = valueStr.substring(0, valueStr.length() - 1);
        return valueList;
    }

}

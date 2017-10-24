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
    private static final String COMMA_SEPARATOR = ",";

    private static Queue<Event> plainQueue;
    private static Queue<Event> encryptedQueue;

    private static ExecutorService encryptWorkers;
    private static ExecutorService encryptBossScheduler;
    private static ScheduledExecutorService encryptedEventsPublishExecutorService;

    private static ScheduledExecutorService logExecutorService;
    private static final int batchSize = 478;
    private static final int maxEmailLength = 40;
    private static final int compositeEventSize = 10;

    private static AtomicLong totalPlainCount = new AtomicLong(0);
    private static AtomicLong totalEncryptedCount = new AtomicLong(0);

    public static void init() throws Exception {
        plainQueue = new ArrayBlockingQueue<>(10000000);
        encryptedQueue = new ArrayBlockingQueue<>(10000000);

        encryptWorkers = Executors.newFixedThreadPool(50);

        encryptBossScheduler = Executors.newSingleThreadExecutor();
        encryptBossScheduler.submit(() -> {
            try {
                while(true) {
                    if(plainQueue.size() > compositeEventSize) {
                        List<Event> events = new ArrayList<>();
                        for(int i=0; i < compositeEventSize; i++) {
                            events.add(plainQueue.poll());
                        }
                        encryptWorkers.submit(() -> {
                            Event encryptedEvent = createCompositeEvent(events);
                            encryptedQueue.add(encryptedEvent);
                            totalEncryptedCount.addAndGet(compositeEventSize);
                        });
                    } else {
                        Thread.sleep(5);
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
//                            log.info("Composite event [" +
//                                    event.getPayloadData()[0] + "," +
//                                    event.getPayloadData()[1] + "," +
//                                    event.getPayloadData()[2] + "," +
//                                    event.getPayloadData()[3] + "," +
//                                    event.getPayloadData()[4] + "," +
//                                    event.getPayloadData()[5] + "," +
//                                    event.getPayloadData()[6] + "," +
//                                    event.getPayloadData()[7] + "]");
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
        StringBuilder field8Builder = new StringBuilder();
        for(Event event : events) {
            Object[] payloadData = event.getPayloadData();
            field1Builder.append(payloadData[0]).append(FIELD_SEPARATOR);
            field5Builder.append(payloadData[4]).append(FIELD_SEPARATOR);
            field6Builder.append(payloadData[5]).append(FIELD_SEPARATOR);
            field7Builder.append(payloadData[6]).append(FIELD_SEPARATOR);
            field8Builder.append(payloadData[7]).append(FIELD_SEPARATOR);

            String from = (String)payloadData[1];
            field2Builder.append(convertToBinaryForm(from, maxEmailLength)).append(COMMA_SEPARATOR);

            String to = (String)payloadData[2];
            String[] toArr = to.split(",");
            field3Builder.append(convertToBinaryForm(toArr[0], maxEmailLength)).append(COMMA_SEPARATOR);

            String cc = (String)payloadData[3];
            String[] ccArr = cc.split(",");
            field4Builder.append(convertToBinaryForm(ccArr[0], maxEmailLength)).append(COMMA_SEPARATOR);
        }

        Object[] modifiedPayload = new Object[8];
        modifiedPayload[0] = field1Builder.toString().replaceAll(FIELD_SEPARATOR + "$", "");
        modifiedPayload[4] = field5Builder.toString().replaceAll(FIELD_SEPARATOR + "$", "");
        modifiedPayload[5] = field6Builder.toString().replaceAll(FIELD_SEPARATOR + "$", "");
        modifiedPayload[6] = field7Builder.toString().replaceAll(FIELD_SEPARATOR + "$", "");
        modifiedPayload[7] = field8Builder.toString().replaceAll(FIELD_SEPARATOR + "$", "");

        int remainingSlots = batchSize - (compositeEventSize * maxEmailLength);
        for(int i = 0; i < remainingSlots; i++) {
            field2Builder.append(0);
            field2Builder.append(",");
            field3Builder.append(0);
            field3Builder.append(",");
            field4Builder.append(0);
            field4Builder.append(",");
        }

        String field2 = field2Builder.toString().replaceAll(",$", "");
        String encryptedField2 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field2);
        modifiedPayload[1] = encryptedField2;

        String field3 = field3Builder.toString().replaceAll(",$", "");
        String encryptedField3 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field3);
        modifiedPayload[2] = encryptedField3;

        String field4 = field4Builder.toString().replaceAll(",$", "");
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
        String valueList = valueBuilder.toString().replaceAll(",$", "");
        return valueList;
    }

}

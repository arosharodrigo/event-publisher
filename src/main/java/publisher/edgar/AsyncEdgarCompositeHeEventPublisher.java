package publisher.edgar;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.Event;
import publisher.ResearchEventPublisher;
import publisher.filter.AsyncCompositeHeEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncEdgarCompositeHeEventPublisher {

    private static Log log = LogFactory.getLog(AsyncCompositeHeEventPublisher.class);

    private static final String FIELD_SEPARATOR = "###";
    private static final String COMMA_SEPARATOR = ",";

    private static Queue<Event> plainQueue;
    private static Queue<Event> encryptedQueue;

    private static ExecutorService encryptWorkers;
    private static ExecutorService encryptBossScheduler;
    private static ScheduledExecutorService encryptedEventsPublisher;

    private static ScheduledExecutorService eventCountPrinter;

    private static ScheduledExecutorService heEventLimiter;
    private static AtomicLong heEventsReceivedForPeriod = new AtomicLong(0);
    private static int currentWaitingCountBeforeDelegatePrivate = 0;
    private static int maxWaitingCountBeforeDelegatePrivate = 10;
    private static long totalDelegatePrivateCount = 0;

    private static final int maxHeEventsReceivedForPeriod = 150;

    private static final int batchSize = 478;
    private static final int maxFieldLength = 20;
    private static final int compositeEventSize = 23;

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
                        currentWaitingCountBeforeDelegatePrivate = 0;
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
                        if(plainQueue.size() > 0 && heEventsReceivedForPeriod.get() == 0) {
                            currentWaitingCountBeforeDelegatePrivate++;
                            if(currentWaitingCountBeforeDelegatePrivate >= maxWaitingCountBeforeDelegatePrivate) {
                                for(int i = 0; i < plainQueue.size(); i++) {
                                    totalDelegatePrivateCount++;
                                    ResearchEventPublisher.sendThroughPrivatePublisher(plainQueue.poll(), "inputEdgarStream:1.0.0");
                                }
                                currentWaitingCountBeforeDelegatePrivate = 0;
                            } else {
                                // Nothing to do
                            }
                        }
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
                        ResearchEventPublisher.sendThroughVm1Publisher(event);
                    }
                } else {
                    // Nothing to do
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 5000, 10, TimeUnit.MILLISECONDS);

        eventCountPrinter = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Event-Count-Printer").build());
        eventCountPrinter.scheduleAtFixedRate(() -> {
            log.info("Plain queue size [" + plainQueue.size() + "], Total Plain Count [" + totalPlainCount.get() + "], " +
                    "Private VM Delegated Count [" + totalDelegatePrivateCount + "], Encrypted queue size [" + encryptedQueue.size() + "], " +
                    "Total Encrypted count [" + totalEncryptedCount.get() + "]");
        }, 5000, 5000, TimeUnit.MILLISECONDS);

        heEventLimiter = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Event-Count-Printer").build());
        heEventLimiter.scheduleAtFixedRate(() -> {
            heEventsReceivedForPeriod.set(0);
        }, 1000, 100, TimeUnit.MILLISECONDS);

    }

    public static boolean addToQueue(Event event) {
        if(heEventsReceivedForPeriod.get() > maxHeEventsReceivedForPeriod) {
            return false;
        } else {
            plainQueue.add(event);
            totalPlainCount.incrementAndGet();
            heEventsReceivedForPeriod.incrementAndGet();
            return true;
        }
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
        StringBuilder field9Builder = new StringBuilder();
        StringBuilder field10Builder = new StringBuilder();
        StringBuilder field11Builder = new StringBuilder();
        StringBuilder field12Builder = new StringBuilder();
        StringBuilder field13Builder = new StringBuilder();
        StringBuilder field14Builder = new StringBuilder();
        StringBuilder field15Builder = new StringBuilder();
        StringBuilder field16Builder = new StringBuilder();

        for(Event event : events) {
            Object[] payloadData = event.getPayloadData();
            field1Builder.append(payloadData[0]).append(FIELD_SEPARATOR);
            field2Builder.append(payloadData[1]).append(FIELD_SEPARATOR);
            field4Builder.append(payloadData[3]).append(FIELD_SEPARATOR);
            field5Builder.append(payloadData[4]).append(FIELD_SEPARATOR);
            field6Builder.append(payloadData[5]).append(FIELD_SEPARATOR);
            field7Builder.append(payloadData[6]).append(FIELD_SEPARATOR);
            field10Builder.append(payloadData[9]).append(FIELD_SEPARATOR);
            field11Builder.append(payloadData[10]).append(FIELD_SEPARATOR);
            field12Builder.append(payloadData[11]).append(FIELD_SEPARATOR);
            field13Builder.append(payloadData[12]).append(FIELD_SEPARATOR);
            field14Builder.append(payloadData[13]).append(FIELD_SEPARATOR);
            field15Builder.append(payloadData[14]).append(FIELD_SEPARATOR);
            field16Builder.append(payloadData[15]).append(FIELD_SEPARATOR);

            String date = (String)payloadData[2];
            field3Builder.append(convertToBinaryForm(date, maxFieldLength)).append(COMMA_SEPARATOR);

            String extension = (String)payloadData[7];
            field8Builder.append(convertToBinaryForm(extension, maxFieldLength)).append(COMMA_SEPARATOR);

            String code = (String)payloadData[8];
            field9Builder.append(convertToBinaryForm(code, maxFieldLength)).append(COMMA_SEPARATOR);
        }

        Object[] modifiedPayload = new Object[16];
        modifiedPayload[0] = removeAdditionalTailSeparator(field1Builder);
        modifiedPayload[1] = removeAdditionalTailSeparator(field2Builder);
        modifiedPayload[3] = removeAdditionalTailSeparator(field4Builder);
        modifiedPayload[4] = removeAdditionalTailSeparator(field5Builder);
        modifiedPayload[5] = removeAdditionalTailSeparator(field6Builder);
        modifiedPayload[6] = removeAdditionalTailSeparator(field7Builder);
        modifiedPayload[9] = removeAdditionalTailSeparator(field10Builder);
        modifiedPayload[10] = removeAdditionalTailSeparator(field11Builder);
        modifiedPayload[11] = removeAdditionalTailSeparator(field12Builder);
        modifiedPayload[12] = removeAdditionalTailSeparator(field13Builder);
        modifiedPayload[13] = removeAdditionalTailSeparator(field14Builder);
        modifiedPayload[14] = removeAdditionalTailSeparator(field15Builder);
        modifiedPayload[15] = removeAdditionalTailSeparator(field16Builder);


        int remainingSlots = batchSize - (compositeEventSize * maxFieldLength);
        for(int i = 0; i < remainingSlots; i++) {
            field3Builder.append(0);
            field3Builder.append(",");
            field8Builder.append(0);
            field8Builder.append(",");
            field9Builder.append(0);
            field9Builder.append(",");
        }

        String field3Str = field3Builder.toString();
        String field3 = field3Str.substring(0, field3Str.length() - 1);
        String encryptedField3 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field3);
        modifiedPayload[2] = encryptedField3;

        String field8Str = field8Builder.toString();
        String field8 = field8Str.substring(0, field8Str.length() - 1);
        String encryptedField8 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field8);
        modifiedPayload[7] = encryptedField8;

        String field9Str = field9Builder.toString();
        String field9 = field9Str.substring(0, field9Str.length() - 1);
        String encryptedField9 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field9);
        modifiedPayload[8] = encryptedField9;

        return new Event(events.get(0).getStreamId(), events.get(0).getTimeStamp(), null, null, modifiedPayload);
    }

    private static String removeAdditionalTailSeparator(StringBuilder fieldBuilder) {
        String fieldStr = fieldBuilder.toString();
        return fieldStr.substring(0, fieldStr.length() - 3);
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

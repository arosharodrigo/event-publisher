package publisher.edgar;

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

public class AsyncEdgarCompositeHeEventPublisher2 {

    private static Log log = LogFactory.getLog(AsyncEdgarCompositeHeEventPublisher2.class);

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

    private static final int maxHeEventsReceivedForPeriod = 2000;

    private static final int batchSize = 168;
    private static final int maxFieldLength = 1;
    private static final int compositeEventSize = 168;

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
        StringBuilder field5Builder1 = new StringBuilder();
        StringBuilder field5Builder2 = new StringBuilder();
        StringBuilder field6Builder = new StringBuilder();
        StringBuilder field7Builder = new StringBuilder();
        StringBuilder field8Builder = new StringBuilder();
        StringBuilder field9Builder = new StringBuilder();
        StringBuilder field10Builder = new StringBuilder();
        StringBuilder field11Builder = new StringBuilder();
        StringBuilder field12Builder = new StringBuilder();
        StringBuilder field13Builder = new StringBuilder();
        StringBuilder field14Builder1 = new StringBuilder();
        StringBuilder field14Builder2 = new StringBuilder();
        StringBuilder field15Builder = new StringBuilder();
        StringBuilder field16Builder = new StringBuilder();

        for(Event event : events) {
            Object[] payloadData = event.getPayloadData();
            field1Builder.append(payloadData[0]).append(FIELD_SEPARATOR);
            field2Builder.append(payloadData[1]).append(FIELD_SEPARATOR);
            field3Builder.append(payloadData[2]).append(FIELD_SEPARATOR);
            field4Builder.append(payloadData[3]).append(FIELD_SEPARATOR);
            field6Builder.append(payloadData[5]).append(FIELD_SEPARATOR);
            field7Builder.append(payloadData[6]).append(FIELD_SEPARATOR);
            field8Builder.append(payloadData[7]).append(FIELD_SEPARATOR);
            field9Builder.append(payloadData[8]).append(FIELD_SEPARATOR);
            field10Builder.append(payloadData[9]).append(FIELD_SEPARATOR);
            field11Builder.append(payloadData[10]).append(FIELD_SEPARATOR);
            field12Builder.append(payloadData[11]).append(FIELD_SEPARATOR);
            field13Builder.append(payloadData[12]).append(FIELD_SEPARATOR);
            field15Builder.append(payloadData[14]).append(FIELD_SEPARATOR);
            field16Builder.append(payloadData[15]).append(FIELD_SEPARATOR);

            Integer zone = (Integer)payloadData[4];
            String binaryStringZone = Integer.toBinaryString(zone);
            int binaryStringZoneLength = binaryStringZone.length();
            field5Builder1.append(binaryStringZone.charAt(binaryStringZoneLength -1)).append(COMMA_SEPARATOR);
            if(binaryStringZoneLength > 1) {
                field5Builder2.append(binaryStringZone.charAt(binaryStringZoneLength -2)).append(COMMA_SEPARATOR);
            } else {
                field5Builder2.append("0").append(COMMA_SEPARATOR);
            }

            Integer find = (Integer)payloadData[13];
            String binaryStringFind = Integer.toBinaryString(find);
            int binaryStringFindLength = binaryStringFind.length();
            field14Builder1.append(binaryStringFind.charAt(binaryStringFindLength -1)).append(COMMA_SEPARATOR);
            if(binaryStringFindLength > 1) {
                field14Builder2.append(binaryStringFind.charAt(binaryStringFindLength -2)).append(COMMA_SEPARATOR);
            } else {
                field14Builder2.append("0").append(COMMA_SEPARATOR);
            }
        }

        Object[] modifiedPayload = new Object[18];
        modifiedPayload[0] = removeAdditionalTailSeparator(field1Builder);
        modifiedPayload[1] = removeAdditionalTailSeparator(field2Builder);
        modifiedPayload[2] = removeAdditionalTailSeparator(field3Builder);
        modifiedPayload[3] = removeAdditionalTailSeparator(field4Builder);
        modifiedPayload[6] = removeAdditionalTailSeparator(field6Builder);
        modifiedPayload[7] = removeAdditionalTailSeparator(field7Builder);
        modifiedPayload[8] = removeAdditionalTailSeparator(field8Builder);
        modifiedPayload[9] = removeAdditionalTailSeparator(field9Builder);
        modifiedPayload[10] = removeAdditionalTailSeparator(field10Builder);
        modifiedPayload[11] = removeAdditionalTailSeparator(field11Builder);
        modifiedPayload[12] = removeAdditionalTailSeparator(field12Builder);
        modifiedPayload[13] = removeAdditionalTailSeparator(field13Builder);
        modifiedPayload[16] = removeAdditionalTailSeparator(field15Builder);
        modifiedPayload[17] = removeAdditionalTailSeparator(field16Builder);

        String field5Str1 = field5Builder1.toString();
        String field51 = field5Str1.substring(0, field5Str1.length() - 1);
        String encryptedField51 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field51);
        modifiedPayload[4] = encryptedField51;

        String field5Str2 = field5Builder2.toString();
        String field52 = field5Str2.substring(0, field5Str2.length() - 1);
        String encryptedField52 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field52);
        modifiedPayload[5] = encryptedField52;

        String field14Str1 = field14Builder1.toString();
        String field141 = field14Str1.substring(0, field14Str1.length() - 1);
        String encryptedField141 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field141);
        modifiedPayload[14] = encryptedField141;

        String field14Str2 = field14Builder2.toString();
        String field142 = field14Str2.substring(0, field14Str2.length() - 1);
        String encryptedField142 = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(field142);
        modifiedPayload[15] = encryptedField142;

        return new Event(events.get(0).getStreamId(), events.get(0).getTimeStamp(), null, null, modifiedPayload);
    }

    private static String removeAdditionalTailSeparator(StringBuilder fieldBuilder) {
        String fieldStr = fieldBuilder.toString();
        return fieldStr.substring(0, fieldStr.length() - 3);
    }

}


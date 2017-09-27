package publisher.filter;

import publisher.ResearchEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class AsyncCompositeHeEventPublisher {

    private static Queue<Object[]> queue;
    private static ScheduledExecutorService scheduledExecutorService;
    private static final int batchSize = 478;

    public static void init() {
        queue = new ArrayBlockingQueue<>(100000);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        /*scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    int currentQueueSize = queue.size();
                    int iterationCount = (currentQueueSize > batchSize) ? batchSize : currentQueueSize;
                    final List<Object[]> eventList = new ArrayList<>();
                    for(int i=0; i<iterationCount; i++) {
                        Object[] event = queue.poll();
                        eventList.add(event);
                    }

                                StringBuilder timestampBuilder = new StringBuilder();
                                StringBuilder valueBuilder = new StringBuilder();
                                for (Object[] event : eventList) {
                                    timestampBuilder.append(event[0]);
                                    timestampBuilder.append(",");
                                    valueBuilder.append(event[1]);
                                    valueBuilder.append(",");
                                }
                                int dummyCount = batchSize - eventList.size();
                                for (int i = 0; i < dummyCount; i++) {
                                    valueBuilder.append(0);
                                    valueBuilder.append(",");
                                }
                                String valueList = valueBuilder.toString().replaceAll(",$", "");
                                String encryptedValueList = ResearchEventPublisher.homomorphicEncDecService.encryptLongVector(valueList);
                                Object[] compositeEvent = {timestampBuilder.toString().replaceAll(",$", ""), encryptedValueList, String.valueOf(eventList.size())};
                                ResearchEventPublisher.publishCompositeEvent(compositeEvent);

                } catch (Throwable t) {
                    System.out.println("Error 7 - " + t);
                    t.printStackTrace();
                }
            }
        }, 10000, 50, TimeUnit.MILLISECONDS);*/

    }

    public static void addToQueue(Object[] payload) {
        queue.add(payload);
    }

}

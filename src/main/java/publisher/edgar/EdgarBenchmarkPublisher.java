package publisher.edgar;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import publisher.Publishable;
import publisher.ResearchEventPublisher;
import publisher.email.EventWrapper;
import publisher.util.Configuration;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EdgarBenchmarkPublisher extends Publishable {

    private static final Log log = LogFactory.getLog(EdgarBenchmarkPublisher.class);

    private Queue<EventWrapper> eventQueue = new ArrayBlockingQueue<>(1000000);
    private AtomicLong messageSize = new AtomicLong(0);

    public EdgarBenchmarkPublisher() {
        super("inputEdgarStream:1.0.0", Configuration.getProperty("data.path.edgar"));
    }

    @Override
    public void startPublishing() throws IOException {
        Reader reader = new FileReader(getDataFilePath());
        Iterable<CSVRecord> allRecords = CSVFormat.EXCEL.parse(reader);
        Iterator<CSVRecord> recordsIterator = allRecords.iterator();

        ScheduledExecutorService dataProducerScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Edgar-Data-Producer").build());
        dataProducerScheduler.scheduleAtFixedRate(() -> {
            try {
                for(int i = 0;i < 20000;i++) {
                    if(recordsIterator.hasNext()) {
                        EventWrapper event = createRecord(recordsIterator.next());
                        eventQueue.add(event);
                    } else {
                        log.info("No Edgar data to read, hope all are read");
                    }
                }
            } catch (Throwable t) {
                log.error("Error while reading Edgar data - " + t);
                t.printStackTrace();
            }
        }, 2000, 10, TimeUnit.MILLISECONDS);

        ScheduledExecutorService edgarDataPublisherScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Edgar-Data-Publisher").build());
        edgarDataPublisherScheduler.scheduleAtFixedRate(() -> {
            try {
                int iterations = 20000;
                int repeatCount = 1;
                for(int i = 0;i < iterations;i++) {
                    EventWrapper event = eventQueue.poll();
                    if(event != null) {
                        for (int j = 0; j < repeatCount; j++) {
                            ResearchEventPublisher.publishEvent(event.getEvent(), getStreamId());
                            int eventsSize = event.getEventSizeInBytes();
                            messageSize.addAndGet(eventsSize);
                        }
                    }
                }
            } catch (Throwable t) {
                System.out.println("Error 6 - " + t);
                t.printStackTrace();
            }
        }, 5000, 10, TimeUnit.MILLISECONDS);

        ScheduledExecutorService edgarDataRatePrinter = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Edgar-Data-Rate-Printer").build());
        edgarDataRatePrinter.scheduleAtFixedRate(() -> {
            try {
                long currentMessageSize = messageSize.getAndSet(0);
                log.info("Input data rate: [" + (currentMessageSize/1024) + "]KB per second");
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 5000, 1000, TimeUnit.MILLISECONDS);
    }

    private EventWrapper createRecord(CSVRecord record) {
        int messageSize = 0;

        String ip = record.get(0);
        messageSize += ip.getBytes().length;

        String date = record.get(1);
        messageSize += date.getBytes().length;

        String time = record.get(2);
        messageSize += time.getBytes().length;

        String zone = record.get(3);
        messageSize += zone.getBytes().length;

        String cik = record.get(4);
        messageSize += cik.getBytes().length;

        String accession = record.get(5);
        messageSize += accession.getBytes().length;

        String extension = record.get(6);
        messageSize += extension.getBytes().length;

        String code = record.get(7);
        messageSize += code.getBytes().length;

        String size = record.get(8);
        messageSize += size.getBytes().length;

        String idx = record.get(9);
        messageSize += idx.getBytes().length;

        String norefer = record.get(10);
        messageSize += norefer.getBytes().length;

        String noagent = record.get(11);
        messageSize += noagent.getBytes().length;

        String find = record.get(12);
        messageSize += find.getBytes().length;

        String crawler = record.get(13);
        messageSize += crawler.getBytes().length;

        String browser = record.get(14);
        messageSize += browser.getBytes().length;

        return new EventWrapper(new Object[]{System.currentTimeMillis(), ip, date, time, zone, cik, accession, extension, code, size, idx, norefer,
                noagent, find, crawler, browser}, messageSize);
    }

}

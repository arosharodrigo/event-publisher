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
import publisher.util.EdgarUtil;
import publisher.util.EdgarUtil2;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EdgarBenchmarkPublisher extends Publishable {

    private static final Log log = LogFactory.getLog(EdgarBenchmarkPublisher.class);

    private Queue<EventWrapper> eventQueue = new ArrayBlockingQueue<>(200000);
    private AtomicLong messageSize = new AtomicLong(0);
    private AtomicInteger dataPublisherCounter = new AtomicInteger(0);

    public EdgarBenchmarkPublisher() {
        super("inputEdgarStream:1.0.0", Configuration.getProperty("data.path.edgar"));
    }

    @Override
    public void startPublishing() throws IOException {
        Reader reader = new FileReader(getDataFilePath());
        Iterable<CSVRecord> allRecords = CSVFormat.EXCEL.parse(reader);
        Iterator<CSVRecord> recordsIterator = allRecords.iterator();
        recordsIterator.next();

        ScheduledExecutorService dataProducerScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Edgar-Data-Producer").build());
        dataProducerScheduler.scheduleAtFixedRate(() -> {
            try {
                for(int i = 0;i < 1500;i++) {//465
                    if(recordsIterator.hasNext()) {
                        if(eventQueue.size() < 200000) {
                            EventWrapper event = createRecord(recordsIterator.next());
                            eventQueue.add(event);
                        }
                    } else {
                        log.info("No Edgar data to read, hope all are read");
                    }
                }
            } catch (Throwable t) {
                log.error("Error while reading Edgar data - " + t);
                t.printStackTrace();
            }
        }, 2000, 10, TimeUnit.MILLISECONDS);

        /*ScheduledExecutorService edgarDataPublisherScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Edgar-Data-Publisher").build());
        edgarDataPublisherScheduler.scheduleAtFixedRate(() -> {
            try {
                int iterations = 5;
                int repeatCount = 300;
                repeatCount = EdgarUtil.generateTps(dataPublisherCounter.getAndIncrement());
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
        }, 5000, 10, TimeUnit.MILLISECONDS);*/

        ScheduledExecutorService edgarDataPublisherScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Edgar-Data-Publisher").build());
        edgarDataPublisherScheduler.scheduleAtFixedRate(() -> {
            try {
//                int iterations = 200;
//                int iterations = 42;
                int iterations = EdgarUtil2.generateTps(dataPublisherCounter.getAndIncrement()) * 1 / 3;
                int repeatCount = 8;
                for(int i = 0;i < iterations;i++) {
                    EventWrapper event = eventQueue.poll();
                    if(event != null) {
                        for(int j = 0;j < repeatCount;j++) {
                            ResearchEventPublisher.publishEventEdgar(event.getEvent(), getStreamId());
                            int eventsSize = event.getEventSizeInBytes();
                            messageSize.addAndGet(eventsSize);
                        }
                    } else {
//                        log.error("No data to publish, reader is slow");
                    }
                }
            } catch (Throwable t) {
                System.out.println("Error at publish boss scheduler - " + t);
                t.printStackTrace();
            }
        }, 4000, 10, TimeUnit.MILLISECONDS);

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

        int code = (record.get(7).isEmpty()) ? 0 : Double.valueOf(record.get(7)).intValue();
        messageSize += 4;

        String size = record.get(8);
        messageSize += size.getBytes().length;

        int idx = (record.get(9).isEmpty()) ? 0 : Double.valueOf(record.get(9)).intValue();
        messageSize += 4;

        int norefer = (record.get(10).isEmpty()) ? 0 : Double.valueOf(record.get(10)).intValue();
        messageSize += 4;

        String noagent = record.get(11) + ",EosvmGSOsDxTQIcDMnES2Hpboy6JIz0T3xAiLq1YofiC9EHW2lZ0lxZJzr9Eron0MQFs2A6qCORtBz5WLWXn2ypzzMnwDqvX0Ej0cZ428HnCzP0O6M2ReNnl6N6nlkVtyYBZ78Lbxsrq0v5N3pKSqlg7mJ1KUsPSaMfGwFGKebEee5k4KfgqabJT86nlfjT21cg7rTTOh8GzmALScl3hprHYSjkLQFGOeCR68Fb7D3Drl2GrT98YS7Ui6WYTitJSLczQ35oHGVmN6xIJxED57oaEvRHxEKbMbucs7xEuQPR4cMKymRPF58S8ijV0OJ2yDfRC4Vx3zw1sMqHZGlyL7kL34EsE3EhcLyGFZSRU5lDrao6QBxH8uZNG4lmHY1sPrchKbRexZSvhG1PqbA918OeTinIYZckLB1oXRH4vlbeBFht7iiBMOBpJKz57JuboizzF0j7CFOwBS1haDuB9P4sxZHDheKkSaB3wMsx3hCzrM3p8LYBHITb0GEKqRbY2JKgBavMGMZ1o1DQj8k6673ivmZvuLcJ0W9oTG9f1AImy979Xgu5iTx2wBmIp4S4VEb3COfCMQ6LDaB21xQFas9iwx3uRf18LIP66RIr3m0wJyymOTwkCtq8fqQ8Us6bwFzWsgbGfOl5zYn73J0Ai4FTvOelpgECrftLXXSGJa5JUrNxjCWqprItHNit9Bw6YaIJFWCpjaI2D1NM86GQtatEgyL2yw811HzEwjha5yw26SVGxqN33Z0Gej6VNJhujlWmlj6kjGerVSJWjPCHweCDtCeJMY1FExzJbaGiFLg0iKsDJG8gXYIvhvbfF4kia2ilr8Z15tNlVcKA2wuf5czwJV6F1kIkjPG2qLKfDGJ6q8cDQgTnph5Qh8GqAeLjYDXUg7qa8D1RSfmqVoH8nQE6qCbY2jaYYsEKYxmZczvpMCblMAm3rCkrcsaeTHzJTacoEXh0tMMOKkXzDIR8wY4tm1LF9yfec9LC9TyyiQXjUugoKoPTlsC3QHhmgc27";
        messageSize += noagent.getBytes().length;

        int find = (record.get(12).isEmpty()) ? 0 : Double.valueOf(record.get(12)).intValue();
        messageSize += 4;

        String crawler = record.get(13);
        messageSize += crawler.getBytes().length;

        String browser = record.get(14);
        messageSize += browser.getBytes().length;

        return new EventWrapper(new Object[]{System.currentTimeMillis(), ip, date, time, zone, cik, accession, extension, code, size, idx, norefer,
                noagent, find, crawler, browser}, messageSize);
    }

}

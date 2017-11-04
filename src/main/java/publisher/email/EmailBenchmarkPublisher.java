package publisher.email;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import publisher.Publishable;
import publisher.ResearchEventPublisher;
import publisher.util.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EmailBenchmarkPublisher extends Publishable {

    private static final Log log = LogFactory.getLog(EmailBenchmarkPublisher.class);

    private Queue<EventWrapper> eventQueue = new ArrayBlockingQueue<>(1000000);
    private AtomicLong messageSize = new AtomicLong(0);

    public EmailBenchmarkPublisher() {
        super("inputEmailsStream:1.0.0", Configuration.getProperty("data.path.email"));
        //super("reducedEmailInputStream:1.0.0", "/home/sajith/research/email-benchmark/EmailDataSet/enron.avro");
    }

    @Override
    public void startPublishing() throws IOException {
        DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<>(MailRecord.class);
        final DataFileReader<MailRecord> dataFileReader = new DataFileReader<>(new File(getDataFilePath()), userDatumReader);

        ScheduledExecutorService emailDataProducerScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Email-Data-Producer").build());
        emailDataProducerScheduler.scheduleAtFixedRate(() -> {
                try {
                    if(dataFileReader.hasNext()) {
                        for(int i = 0;i < 5;i++) {
                            readData(dataFileReader);
                        }
                    } else {
                        log.info("No Email data to read, hope all are read");
                    }
                } catch (Throwable t) {
                    System.out.println("Error while reading Email data - " + t);
                    t.printStackTrace();
                }
        }, 2000, 10, TimeUnit.MILLISECONDS);

        ScheduledExecutorService emailDataPublisherScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Email-Data-Publisher").build());
        emailDataPublisherScheduler.scheduleAtFixedRate(() -> {
                try {
                    int iterations = 5;
                    int repeatCount = 90;
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

        ScheduledExecutorService emailDataRatePrinter = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Email-Data-Rate-Printer").build());
        emailDataRatePrinter.scheduleAtFixedRate(() -> {
                try {
                    long currentMessageSize = messageSize.getAndSet(0);
                    log.info("Input data rate: [" + (currentMessageSize/1024) + "]KB per second");
                } catch (Throwable t) {
                    t.printStackTrace();
                }
        }, 5000, 1000, TimeUnit.MILLISECONDS);
    }

    private void readData(DataFileReader<MailRecord> dataFileReader) throws UnsupportedEncodingException {
        MailRecord email;
        String toAddresses;
        String ccAddresses;
        String bccAddresses;
        String subject;
        String body;
        String from;
        int messageSize = 8;

        email = dataFileReader.next();

        Iterator<CharSequence> itr = null;
        StringBuilder sb = new StringBuilder();

        final List<CharSequence> to = email.getTo();
        if(to != null && !to.isEmpty()) {
            sb.append(to.get(0));
        }

        toAddresses = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
        messageSize += toAddresses.getBytes().length;
        sb = new StringBuilder();

        final List<CharSequence> cc = email.getCc();
        if(cc != null && !cc.isEmpty()) {
            sb.append(cc.get(0));
        }

        ccAddresses = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
        messageSize += ccAddresses.getBytes().length;
        sb = new StringBuilder();

        final List<CharSequence> bcc = email.getBcc();
        if(bcc != null && !bcc.isEmpty()) {
            sb.append(bcc.get(0));
        }

        bccAddresses = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
        messageSize += bccAddresses.getBytes().length;

        subject = new String(email.getSubject().toString().getBytes("ISO-8859-1"),"UTF-8");
        messageSize += subject.getBytes().length;

        body = new String(email.getBody().toString().getBytes("ISO-8859-1"),"UTF-8");
        messageSize += body.getBytes().length;

        from = new String(email.getFrom().toString().getBytes("ISO-8859-1"), "UTF-8");
        messageSize += from.getBytes().length;

        eventQueue.add(new EventWrapper(new Object[]{System.currentTimeMillis(), from, toAddresses, ccAddresses, bccAddresses, subject, body}, messageSize));
    }
}

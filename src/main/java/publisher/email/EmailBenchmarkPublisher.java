package publisher.email;

import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import publisher.Publishable;
import publisher.ResearchEventPublisher;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by sajith on 7/24/16.
 */
public class EmailBenchmarkPublisher extends Publishable implements Runnable{

    private int sentCount = 0;
    Object[] outofOrderEvent = null;
    int collisionCount = 0;
    Map<Integer, String> hashCodes = new HashMap<>();

    public EmailBenchmarkPublisher() {
        super("inputEmailsStream:1.0.0", "/home/sajith/research/email-benchmark/EmailDataSet/enron.avro");
        //super("reducedEmailInputStream:1.0.0", "/home/sajith/research/email-benchmark/EmailDataSet/enron.avro");
    }

    public void publish(Object[] event, int messageSize) throws InterruptedException {

        ResearchEventPublisher.publishEvent(event, getStreamId());
        ResearchEventPublisher.addMessageSize(messageSize);

        //====For Out Of Order Event Sending
        /*
        if (sentCount % 100 == 0){
            if (outofOrderEvent != null){
                ResearchEventPublisher.sendOutofOrder(outofOrderEvent, getStreamId(), true);
            }
            outofOrderEvent = event;
        } else {
            ResearchEventPublisher.sendOutofOrder(outofOrderEvent, getStreamId(), false);
        }
        sentCount++;
        */
        //=================================


        /*
        ResearchEventPublisher.publishMultiplePublishers(event, getStreamId(), ResearchEventPublisher.EMAIL_PROCESSOR_ID);
        sentCount++;
        if (sentCount % 10800 == 0){
            Thread.sleep(1 * 1000);
            //System.out.println(sentCount + "Events sent in Email Processor benchmark");
        }
        */

    }

    public  void checkHashcodes(String value){
        String currentValue = hashCodes.get(value.hashCode());
        if (currentValue != null){
            if (!currentValue.equals(value)){
                collisionCount++;
                System.out.println("Collision!!! HasCode="+ value.hashCode() + "\nFirstValue=" + currentValue + "\nSecondValue=" + value);
            }
        } else {
            hashCodes.put(value.hashCode(), value);
        }

    }

    @Override
    public void startPublishing() {
        Thread publisherThread = new Thread(this);
        publisherThread.start();
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p/>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {

            String toAddresses;
            String ccAddresses;
            String bccAddresses;
            String from;
            String body;
            String subject;

            DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<MailRecord>(MailRecord.class);
            DataFileReader<MailRecord> dataFileReader = new DataFileReader<MailRecord>(new File(getDataFilePath()), userDatumReader);
            MailRecord email = null;

            System.out.println("waiting for user input");
            System.in.read();

            long start = System.currentTimeMillis();
            int count = 0;
            while (dataFileReader.hasNext()) {
                int messageSize = (new String("(.*)@enron.com").getBytes("UTF-8").length) + 4;

                count++;
                email = dataFileReader.next();

                Iterator<CharSequence> itr = null;
                StringBuilder sb = new StringBuilder();

                final List<CharSequence> to = email.getTo();
                if(to != null) {
                    itr = to.iterator();

                    while (itr.hasNext()) {
                        sb.append(itr.next());
                        if(itr.hasNext()){
                            sb.append(",");
                        }
                    }
                }


                toAddresses = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
                messageSize += toAddresses.getBytes().length;
                sb = new StringBuilder();

                final List<CharSequence> cc = email.getCc();
                if(cc != null) {
                    itr = cc.iterator();

                    while (itr.hasNext()) {
                        sb.append(itr.next());
                        if(itr.hasNext()){
                            sb.append(",");
                        }
                    }
                }

                ccAddresses = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
                messageSize += ccAddresses.getBytes().length;
                sb = new StringBuilder();

                final List<CharSequence> bcc = email.getBcc();
                if(bcc != null) {
                    itr = bcc.iterator();

                    while (itr.hasNext()) {
                        sb.append(itr.next());
                        if(itr.hasNext()){
                            sb.append(",");
                        }
                    }
                }

                bccAddresses = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
                messageSize += bccAddresses.getBytes().length;

                subject = new String(email.getSubject().toString().getBytes("ISO-8859-1"),"UTF-8");
                messageSize += subject.getBytes().length;

                body = new String(email.getBody().toString().getBytes("ISO-8859-1"),"UTF-8");
                messageSize += body.getBytes().length;

                from = new String(email.getFrom().toString().getBytes("ISO-8859-1"), "UTF-8");
                messageSize += from.getBytes().length;

                messageSize -= 7;


                for (int i = 0; i < 100; i ++) {
                    publish(new Object[]{System.currentTimeMillis(), from, toAddresses, ccAddresses, bccAddresses, subject, body, "(.*)@enron.com"}, messageSize);

                    /*
                    publish(new Object[]{System.currentTimeMillis(),
                            from,
                            Compressor.compress(toAddresses),
                            Compressor.compress(ccAddresses),
                            Compressor.compress(bccAddresses),
                            subject,
                            Compressor.compress(body),
                            "(.*)@enron.com"});
                            */
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}

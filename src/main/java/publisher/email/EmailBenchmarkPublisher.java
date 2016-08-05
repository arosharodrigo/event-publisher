package publisher.email;

import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import publisher.Publishable;
import publisher.ResearchEventPublisher;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sajith on 7/24/16.
 */
public class EmailBenchmarkPublisher extends Publishable{

    public EmailBenchmarkPublisher() {
        super("inputEmailsStream:1.0.0", "/home/sajith/research/email-benchmark/EmailDataSet/enron.avro");
    }

    @Override
    public void startPublishing() {
        try {
            String toAddresses = null;
            String ccAddresses = null;
            String bccAddresses = null;
            String from = null;
            String body = null;
            String subject = null;

            DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<MailRecord>(MailRecord.class);
            DataFileReader<MailRecord> dataFileReader = new DataFileReader<MailRecord>(new File(getDataFilePath()), userDatumReader);
            MailRecord email = null;

            System.out.println("waiting for user input");
            System.in.read();

            int count = 0;
            while (dataFileReader.hasNext()) {
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
                subject = new String(email.getSubject().toString().getBytes("ISO-8859-1"),"UTF-8");
                body = new String(email.getBody().toString().getBytes("ISO-8859-1"),"UTF-8");
                from = new String(email.getFrom().toString().getBytes("ISO-8859-1"), "UTF-8");

                //events.add(new Object[]{System.currentTimeMillis(), from, toAddresses, ccAddresses, bccAddresses, subject, body, "(.*)@enron.com"});
                for (int i = 0; i < 100; i ++) {
                    ResearchEventPublisher.publishEvent(new Object[]{System.currentTimeMillis(), from, toAddresses, ccAddresses, bccAddresses, subject, body, "(.*)@enron.com"},
                            getStreamId());
                }

            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}

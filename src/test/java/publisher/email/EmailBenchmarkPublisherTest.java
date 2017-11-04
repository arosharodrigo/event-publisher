package publisher.email;

import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import publisher.util.Configuration;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class EmailBenchmarkPublisherTest {

    @Test
    public void test() throws Exception {
//        AtomicLong emailCounter = new AtomicLong(0);
//        EmailBenchmarkPublisher publisher = new EmailBenchmarkPublisher();
//        DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<>(MailRecord.class);
//        final DataFileReader<MailRecord> dataFileReader = new DataFileReader<>(new File(Configuration.getProperty("data.path.email")), userDatumReader);
//        while(dataFileReader.hasNext()) {
//            emailCounter.incrementAndGet();
//            EventWrapper eventWrapper = publisher.readData(dataFileReader);
//            if(emailCounter.get() % 100000 == 0) {
//                System.out.println(emailCounter.get() + " reads done, Max lengths [" + publisher.getFromMax().get() + "|" + publisher.getToMax().get() + "|" + publisher.getCcMax().get() + "|" + publisher.getBccMax().get() + "]");
//            }
//        }
//        System.out.println(emailCounter.get() + " reads done, Max lengths [" + publisher.getFromMax().get() + "|" + publisher.getToMax().get() + "|" + publisher.getCcMax().get() + "|" + publisher.getBccMax().get() + "]");
    }
}

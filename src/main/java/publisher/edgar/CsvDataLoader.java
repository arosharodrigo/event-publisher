package publisher.edgar;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import publisher.util.Configuration;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class CsvDataLoader implements DataLoader {

    public void readData() throws IOException {

        // MBB option
        //Create file object
        File file = new File(Configuration.getProperty("data.path.edgar"));
        //Get file channel in readonly mode
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
        //Get direct byte buffer access using channel.map() operation

        long fileChannelSize = fileChannel.size();
        int chunkSize = 0x8FFFFFF;
        double iterations = fileChannelSize / (double)chunkSize;
        int requiredIterations = (int)Math.ceil(iterations);
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < requiredIterations; i++) {
            long start = System.currentTimeMillis();
            List<String> lines = new ArrayList<>();
            long calculatedChunkSize = (requiredIterations == i+1) ? fileChannelSize - (i * chunkSize) : chunkSize;
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, (i * chunkSize), calculatedChunkSize);
            for (int j = 0; j < buffer.limit(); j++) {
                byte b = buffer.get();
                if(b == 10) {
                    lines.add(sb.toString());
                    sb = new StringBuilder();
                } else {
                    sb.append((char)b);
                }
            }
            long stop = System.currentTimeMillis();
            System.out.println("Line count for the iteration [" + i + "]: " + lines.size() + ", TPS [" + lines.size()*1000/(stop-start) + "]");
        }
        fileChannel.close();
    }

    public void readData2() throws IOException {
        // Apache commons CSV
        Reader in = new FileReader(Configuration.getProperty("data.path.edgar"));
        Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
        long count = 0;
        for (CSVRecord record : records) {
            count++;
            String ip = record.get(0);
            String accession = record.get(5);
//            System.out.println("IP [" + ip + "], accession [" + accession + "]");
        }
        System.out.println("CSV count: " + count);
        in.close();
    }

}

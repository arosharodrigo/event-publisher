package publisher.schedular.util;

import org.apache.axiom.om.util.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by sajith on 9/19/16.
 */
public class Compressor {
    private static Deflater compressor = new Deflater();
    private static  long totalBytesSaved = 0;
    static  int count = 0;

    public static String compress(String data) throws  IOException {
        count++;
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        byte[] dataBytes = data.getBytes("UTF-8");
        deflater.setInput(dataBytes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(dataBytes.length);
        deflater.finish();
        byte[] buffer = new byte[10 * 1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();

        byte[] output = outputStream.toByteArray();
        String result =  Base64.encode(output);

        int diff = (data.length() - result.length());
        totalBytesSaved += diff;
        if (count % 10000 == 0){
            System.out.println("Total Bytes Saved :" + totalBytesSaved);
        }
        return result;
        //return new String(output, 0, count, "UTF-8");
    }

    public static String decompress(String data) throws IOException, DataFormatException {
        byte[] dataBytes = Base64.decode(data);
        Inflater inflater = new Inflater();
        inflater.setInput(dataBytes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(dataBytes.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }

        outputStream.close();
        byte[] output = outputStream.toByteArray();
        return new String(output);
    }
}

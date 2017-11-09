package publisher.edgar;

import org.junit.Test;

import static org.junit.Assert.*;

public class CsvDataLoaderTest {

    @Test
    public void readData() throws Exception {
        CsvDataLoader csvDataLoader = new CsvDataLoader();
//        csvDataLoader.readData();
        csvDataLoader.readData2();
    }

}
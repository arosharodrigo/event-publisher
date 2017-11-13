package publisher.util;

import org.junit.Test;

public class EdgarUtilTest {

    @Test
    public void testGenerateRandomTps() throws Exception {
        for (int i = 0; i < 100; i++) {
            System.out.println(EdgarUtil.generateRandomTps());
        }
    }
}
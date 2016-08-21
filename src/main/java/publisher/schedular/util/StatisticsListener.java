package publisher.schedular.util;

/**
 * Created by sajith on 8/17/16.
 */
public interface StatisticsListener {

    void onStatisticsRead(double latency, double throughput);
}

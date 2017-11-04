package publisher.schedular.util;

public class LatencyWrapper {

    private long time;
    private double latency;

    public LatencyWrapper(long time, double latency) {
        this.time = time;
        this.latency = latency;
    }

    public long getTime() {
        return time;
    }

    public double getLatency() {
        return latency;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LatencyWrapper{");
        sb.append("time=").append(time);
        sb.append(", latency=").append(latency);
        sb.append('}');
        return sb.toString();
    }
}

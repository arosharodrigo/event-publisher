package publisher.schedular.util;

/**
 * Created by sajith on 8/2/16.
 */
public class Endpoint {
    private String ipAddress;
    private int port;

    public Endpoint(String ipAddress, int port){
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public String getIPAddress(){
        return ipAddress;
    }

    public int getPort(){
        return port;
    }

    @Override
    public String toString(){
        return ipAddress + ":" + port;
    }
}

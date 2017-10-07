package publisher;

import java.io.IOException;

/**
 * Created by sajith on 7/24/16.
 */
public abstract class Publishable {

    String streamId;
    String dataFilePath;

    public Publishable(String streamId, String dataFilePath){
        this.streamId = streamId;
        this.dataFilePath = dataFilePath;
    }

    public String getStreamId(){
        return streamId;
    }

    public String getDataFilePath(){
        return dataFilePath;
    }


    public abstract void startPublishing() throws IOException;
}

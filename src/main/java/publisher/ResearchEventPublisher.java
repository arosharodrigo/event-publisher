/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package publisher;

import org.apache.axiom.om.util.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import publisher.filter.FilterBenchmarkPublisher;
import publisher.schedular.PublicCloudDataPublishManager;
import publisher.schedular.util.Configurations;
import publisher.schedular.util.DataPublisherUtil;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.vm.VMConfig;
import publisher.schedular.vm.VMManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//mvn exec:java -Dexec.mainClass="publisher.ResearchEventPublisher"
public class ResearchEventPublisher implements WrapperListener {
    public static final int EMAIL_PROCESSOR_ID = 1;
    public static final int DEBS_Q1_ID = 2;

    public static final String USER_NAME = "admin";
    public static final String PASSWORD = "admin";

    private static Log log = LogFactory.getLog(ResearchEventPublisher.class);

    private static DataPublisher privateDataPublisher;
    private static DataPublisher currentDataPublisher;

    private static PublicCloudDataPublishManager publicCloudDataPublishManager = new PublicCloudDataPublishManager();
    private static ArrayList<DataPublisher> publicCloudPublishers = new ArrayList<DataPublisher>();
    private static boolean sendToPublicCloud = false;

    private static VMManager vmManager = null;

    private static int currentPublicPublishCount = 0;
    private static int publicSent = 0;
    private static int count = 0;
    private static int totalSentToPublicCloud;

    private static int eventPercentageToBeSentToPublicCloud = 0;
    private static int maxEventPercentageToBeSentToPublicCloud = 15;

    private static int publicCloudPublishingRatioPerVm = 3; // Tells how much events to be published to public cloud for every 1000 events;
    private static boolean isSwitching = true;
    private static int publishingRate = 6000;
    private static int publicCloudPublishBatchSize = 1000;

    private static FilterBenchmarkPublisher publisher;
    private static HomomorphicEncDecService homomorphicEncDecService;

    private static Properties prop;

    static {
        prop = new Properties();
        InputStream input = null;
        try {
            String filename = "config.properties";
            input = StatisticsInputReaderTask.class.getClassLoader().getResourceAsStream(filename);
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        WrapperManager.start(new ResearchEventPublisher(), args);
    }

    private static void initVmManager(){
        List<VMConfig> vmConfigList = new ArrayList<>();

//        vmConfigList.add(new VMConfig(1, Integer.valueOf(prop.getProperty("public.das.vm1.port")), prop.getProperty("public.das.vm1.ip"), 5 * 1000,  10 * 1000, 1000));
//        vmConfigList.add(new VMConfig(1, 9611, "192.248.8.135", 5 * 1000,  10 * 1000, 1000));
        //vmConfigList.add(new VMConfig(2, 7611, "192.168.57.81", 20 * 1000,  22 * 1000, 10 * 1000));
        //vmConfigList.add(new VMConfig(3, 7611, "192.168.57.82", 30 * 1000,  32 * 1000, 10 * 1000));
        //vmConfigList.add(new VMConfig(4, 7611, "192.168.57.85", 40 * 1000,  42 * 1000, 10 * 1000));

        vmManager = new VMManager(vmConfigList);
    }


    public static Object[] compress(Object[] eventPayload){
        // For email processor
        /*try {
            eventPayload[2] = Compressor.compress(eventPayload[2].toString());
            eventPayload[3] = Compressor.compress(eventPayload[3].toString());
            eventPayload[4] = Compressor.compress(eventPayload[4].toString());
            eventPayload[6] = Compressor.compress(eventPayload[6].toString());
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        return eventPayload;
    }

    public static Object[] encrypt(Object[] eventPayload){
        Object[] modifiedPayload = new Object[21];
        try {
            modifiedPayload[0] = eventPayload[0];

            long value = (Long)eventPayload[1];
            byte[] byteArray = homomorphicEncDecService.encrypt(Long.toBinaryString(Long.MIN_VALUE | value).substring(32));

            int partLength = 30000;
            int totalLength = byteArray.length;
            int accumulatedLength = 0;
            int i;
            for(i=1; accumulatedLength < totalLength; i++) {
                byte[] part = new byte[partLength];
                if((accumulatedLength + partLength) <= totalLength) {
                    System.arraycopy(byteArray, accumulatedLength, part, 0, part.length);
                } else {
                    System.arraycopy(byteArray, accumulatedLength, part, 0, (totalLength-accumulatedLength));
                }
                String encryptedValue = Base64.encode(part);
//                String encryptedValue = new String(part, "UTF-8");
                modifiedPayload[i] = encryptedValue;
                accumulatedLength += partLength;
            }
            for (; i < modifiedPayload.length; i++) {
                modifiedPayload[i] = "";
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error2 - " + e);
        }
        return modifiedPayload;
    }

    /*public static byte[] compress(String data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length());
        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        gzip.write(data.getBytes());
        gzip.close();
        byte[] compressed = bos.toByteArray();
        bos.close();
        return compressed;
    }*/

    public static void publishEvent(Object[] eventPayload, String streamId) throws InterruptedException {

        if (sendToPublicCloud && (currentDataPublisher == privateDataPublisher)){
            if (count % (100 - eventPercentageToBeSentToPublicCloud) == 0){
                currentDataPublisher = null; //setting to null for it to be picked interchangeably when  sending event in line # 162
            }
        }

        if (currentDataPublisher == privateDataPublisher){
            eventPayload = encrypt(eventPayload);
            streamId = publisher.getStreamId(true);
        }

        if (currentDataPublisher != privateDataPublisher){
            publicSent++;
            totalSentToPublicCloud++;
//            eventPayload = compress(eventPayload);
//            eventPayload = encrypt(eventPayload);
//            streamId = publisher.getStreamId(true);
        }

        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (currentDataPublisher != privateDataPublisher){
            currentDataPublisher = publicCloudPublishers.get((count % publicCloudPublishBatchSize) % publicCloudPublishers.size());
            currentDataPublisher.tryPublish(event);
        } else {
            currentDataPublisher.publish(event);
       }

        if (currentDataPublisher != privateDataPublisher){
            if (++currentPublicPublishCount == eventPercentageToBeSentToPublicCloud){
                currentDataPublisher = privateDataPublisher;
                currentPublicPublishCount = 0;
            }
        }

        if (++count % publishingRate == 0) {
            Thread.sleep(1000);
        }

        if (count % 100000 == 0){
            log.info("Done Sending " + (float)count/1000000.0  + " M Events[TotalSentToPublicCloud=" + totalSentToPublicCloud + ", PublicCloudSendingRatio=" + eventPercentageToBeSentToPublicCloud + "]");
        }

        if (count == 2500000){
            System.exit(0);
        }
    }

    /**
     * Call back form VM manager to notify that a VM has started
     * @param vmConfig
     */
    public static void OnVmStarted(VMConfig vmConfig){
        publicCloudDataPublishManager.registerVM(vmConfig);
    }

    /**
     * Call back form VM manager to notify that a VM is going to shutdown
     * @param vmConfig
     */
    public static boolean OnVmGoingToShutDown(VMConfig vmConfig){
        boolean keepTheVM = publicCloudDataPublishManager.vmGoingToShutDown(vmConfig, publicSent);
        publicSent = 0;
        return keepTheVM;
    }

    /**
     * Call back form data publish manger to notify that events should be sent to a VM
     * @param vmPublisher
     */
    public static void onSendDataToVM(DataPublisher vmPublisher){
        if (!publicCloudPublishers.contains(vmPublisher)) {
            publicCloudPublishers.add(vmPublisher);
            sendToPublicCloud = true;

            if ((eventPercentageToBeSentToPublicCloud + publicCloudPublishingRatioPerVm) < maxEventPercentageToBeSentToPublicCloud) {
                eventPercentageToBeSentToPublicCloud += publicCloudPublishingRatioPerVm;
            } else {
                eventPercentageToBeSentToPublicCloud = maxEventPercentageToBeSentToPublicCloud;
            }
        }
    }

    /**
     * Call back form data publish manger to notify that events should not be sent to a VM
     * @param vmPublisher
     */
    public static void onStopSendingDataToVM(DataPublisher vmPublisher){
        if (publicCloudPublishers.contains(vmPublisher)) {
            publicCloudPublishers.remove(vmPublisher);

            if (eventPercentageToBeSentToPublicCloud > 0) {
                eventPercentageToBeSentToPublicCloud -= publicCloudPublishingRatioPerVm;
            }

            if (publicCloudPublishers.isEmpty()) {
                sendToPublicCloud = false;
            }
        }
    }

    /*
    public synchronized static void publishMultiplePublishers(Object[] eventPayload, String streamId, int id) throws InterruptedException {

    }*/

    public static void sendOutofOrder(Object[] eventPayload, String streamId, boolean isOutOfOrder) throws InterruptedException {
        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (!isOutOfOrder && sendToPublicCloud){
            publishEvent(eventPayload, streamId);
        } else{
            ++count;
            privateDataPublisher.tryPublish(event);
        }

        if (count % 12000 == 0) {
            Thread.sleep(1000);
        }
    }

    @Override
    public Integer start(String[] strings) {
        System.out.println("=================================================================================");
        System.out.println("==========================Starting Event Publisher===============================");
        System.out.println("=================================================================================");

        homomorphicEncDecService = new HomomorphicEncDecService();
        homomorphicEncDecService.init();

        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        try {
            Configurations.setVmStartDelay(10 * 1000);
            Configurations.setVmBillingSessionDuration(60*1000);
            Configurations.setMinEventsToKeepVm(100000);

            log.debug("Starting WSO2 Event ResearchEventPublisher Stream Client");
            AgentHolder.setConfigPath("conf/files/configs/data-agent-config.xml");
            DataPublisherUtil.setTrustStoreParams();
            DataPublisherUtil.loadStreamDefinitions();

            privateDataPublisher = new DataPublisher(prop.getProperty("protocol"),  prop.getProperty("private.das.receiver.url") , null, USER_NAME, PASSWORD);
            currentDataPublisher = privateDataPublisher;


            if (isSwitching) {
                initVmManager();
                vmManager.start();
            }


            publisher = new FilterBenchmarkPublisher("inputFilterStream:1.0.0", "inputHEFilterStream:1.0.0");
            publisher.startPublishing();

            //Publishable debs2016Query1Publisher = new Debs2016Query1Publisher();
            //debs2016Query1Publisher.startPublishing();
            //Publishable debs2016Query2Publisher = new Debs2016Query2Publisher();
            //debs2016Query2Publisher.startPublishing();
        } catch (Throwable e) {
            log.error(e);
        }
        return null;
    }

    @Override
    public int stop(int i) {
        System.out.println("=================================================================================");
        System.out.println("==========================Stopping Event Publisher===============================");
        System.out.println("=================================================================================");
        return 0;
    }

    @Override
    public void controlEvent(int i) {

    }
}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import publisher.email.EmailBenchmarkPublisher;
import publisher.filter.AsyncCompositeHeEventPublisher;
import publisher.filter.FilterBenchmarkPublisher;
import publisher.schedular.PublicCloudDataPublishManager;
import publisher.schedular.util.Configurations;
import publisher.schedular.util.DataPublisherUtil;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.vm.VMConfig;
import publisher.schedular.vm.VMManager;
import publisher.util.Configuration;
import publisher.util.GzipUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
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
    private static int maxEventPercentageToBeSentToPublicCloud = 1;

    private static int publicCloudPublishingRatioPerVm = 1; // Tells how much events to be published to public cloud for every 1000 events;
    private static boolean isSwitching = false;
    private static int publishingRate = 6000;
    private static int publicCloudPublishBatchSize = 40000;

//    public static FilterBenchmarkPublisher publisher;
    public static EmailBenchmarkPublisher publisher;
    public static HomomorphicEncDecService homomorphicEncDecService;

    private static final int batchSize = 478;

    public static void main(String[] args) throws InterruptedException {
        WrapperManager.start(new ResearchEventPublisher(), args);
    }

    private static void initVmManager(){
        List<VMConfig> vmConfigList = new ArrayList<>();

//        vmConfigList.add(new VMConfig(1, Integer.valueOf(Configuration.getProperty("public.das.vm1.port")), Configuration.getProperty("public.das.vm1.ip"), 4 * 1000,  6 * 1000, 2 * 1000));
//        vmConfigList.add(new VMConfig(1, Integer.valueOf(Configuration.getProperty("public.das.vm1.port")), Configuration.getProperty("public.das.vm1.ip"), 8 * 1000,  10 * 1000, 2 * 1000)); - 40000 tps support and good percent to public VM
//        vmConfigList.add(new VMConfig(1, 9611, "192.248.8.134", 10, 10, 10));
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
        Object[] modifiedPayload = new Object[2];
        try {
            modifiedPayload[0] = eventPayload[0];

            long value = (Long)eventPayload[1];
            String encryptLong = homomorphicEncDecService.encryptLong(value);
            modifiedPayload[1] = encryptLong;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error2 - " + e);
        }
        return modifiedPayload;
    }

    public static Object[] encrypt2(Object[] eventPayload){
        Object[] modifiedPayload = new Object[eventPayload.length];
        try {
            modifiedPayload[0] = eventPayload[0];
            modifiedPayload[4] = eventPayload[4];
            modifiedPayload[5] = eventPayload[5];
            modifiedPayload[6] = eventPayload[6];
            modifiedPayload[7] = eventPayload[7];

            String from = (String)eventPayload[1];
            modifiedPayload[1] = encryptToStr(from, batchSize);
            String to = (String)eventPayload[2];
            String[] toArr = to.split(",");
            modifiedPayload[2] = encryptToStr(toArr[0], batchSize);
            String cc = (String)eventPayload[3];
            String[] ccArr = cc.split(",");
            modifiedPayload[3] = encryptToStr(ccArr[0], batchSize);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error2 - " + e);
        }
        return modifiedPayload;
    }

    private static String encryptToStr(String param, int batchSize) {
        StringBuilder valueBuilder = new StringBuilder();
        byte[] paramBytes = param.getBytes();
        for(byte value : paramBytes) {
            valueBuilder.append(value);
            valueBuilder.append(",");
        }
        int dummyCount = batchSize - paramBytes.length;
        for(int i = 0;i < dummyCount; i++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueList = valueBuilder.toString().replaceAll(",$", "");
        String encryptedParam = homomorphicEncDecService.encryptLongVector(valueList);
//        try {
//            log.info("Encrypted length [" + encryptedParam.length() + "]");
//            byte[] compress = GzipUtil.compress(encryptedParam);
//            String result = Base64.getEncoder().encodeToString(compress);
//            log.info("After compressed length [" + result.length() + "]");
//            byte[] decode = Base64.getDecoder().decode(result);
//            String decompress = GzipUtil.decompress(decode);
//            log.info("Is equal [" + (decompress.equals(encryptedParam)) + "]");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return encryptedParam;
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
//            if(isHeEventMode) {
//                AsyncCompositeHeEventPublisher.addToQueue(eventPayload);
//            } else {
                Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);
                currentDataPublisher.publish(event);

//            streamId = "inputHEEmailsStream:1.0.0";
//            Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);
//            AsyncCompositeHeEventPublisher.addToQueue(event);

//            AsyncCompositeHeEventPublisher.addToQueue(eventPayload);

            //****** Remove this b4 perf test ******//
//            streamId = "inputHEEmailsStream:1.0.0";
//            Event event = new Event(streamId, System.currentTimeMillis(), null, null, encrypt2(eventPayload));
//            currentDataPublisher.publish(event);
//            DataPublisher dataPublisher = PublicCloudDataPublishManager.vmIdToDataPublisher.get(1);
//            dataPublisher.tryPublish(event);
//            currentDataPublisher = publicCloudPublishers.get((count % publicCloudPublishBatchSize) % publicCloudPublishers.size());
//            currentDataPublisher.tryPublish(event);

//            }
        }

        if (currentDataPublisher != privateDataPublisher){
            publicSent++;
            totalSentToPublicCloud++;
//            eventPayload = compress(eventPayload);
//            eventPayload = encrypt(eventPayload);
//            streamId = publisher.getStreamId(true);

//            streamId = "inputHEEmailsStream:1.0.0";
//            Event event = new Event(streamId, System.currentTimeMillis(), null, null, encrypt2(eventPayload));
//            Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

//            DataPublisher dataPublisher = PublicCloudDataPublishManager.vmIdToDataPublisher.get(1);
//            dataPublisher.tryPublish(event);
//            Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);
//            currentDataPublisher = publicCloudPublishers.get((count % publicCloudPublishBatchSize) % publicCloudPublishers.size());
//            currentDataPublisher.tryPublish(event);
            streamId = "inputHEEmailsStream:1.0.0";
            Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);
            AsyncCompositeHeEventPublisher.addToQueue(event);
        }

        if (currentDataPublisher != privateDataPublisher){
            if (++currentPublicPublishCount == eventPercentageToBeSentToPublicCloud){
                currentDataPublisher = privateDataPublisher;
                currentPublicPublishCount = 0;
            }
        }
        ++count;
//        if ( % publishingRate == 0) {
//            Thread.sleep(1000);
//        }

        if (count % 100000 == 0){
            log.info("Done Sending " + (float)count/1000000.0  + " M Events[TotalSentToPublicCloud=" + totalSentToPublicCloud + ", PublicCloudSendingRatio=" + eventPercentageToBeSentToPublicCloud + "]");
        }

        if (count == 100000000){
            System.exit(0);
        }
    }

//    public static void publishCompositeEvent(Object[] compositeEventPayload) {
//        Event event = new Event(publisher.getStreamId(true), System.currentTimeMillis(), null, null, compositeEventPayload);
//        DataPublisher dataPublisher = PublicCloudDataPublishManager.vmIdToDataPublisher.get(1);
//        dataPublisher.tryPublish(event);
//    }

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
        homomorphicEncDecService.init(Configuration.getProperty("key.file.path"));

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
            DataPublisherUtil.setKeyStoreParams();
            DataPublisherUtil.setPseudoCarbonHome();
            DataPublisherUtil.loadStreamDefinitions();

            privateDataPublisher = new DataPublisher(Configuration.getProperty("protocol"),  Configuration.getProperty("private.das.receiver.url") , null, USER_NAME, PASSWORD);
            currentDataPublisher = privateDataPublisher;

            if (isSwitching) {
                initVmManager();
                vmManager.start();
            }
            AsyncCompositeHeEventPublisher.init();

//            publisher = new FilterBenchmarkPublisher("inputFilterStream:1.0.0", "inputHEFilterStream:1.0.0");
//            publisher.startPublishing();

            publisher = new EmailBenchmarkPublisher();
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

    public static void sendThroughPrivatePublisher(Event event) {
        currentDataPublisher.publish(event);
    }
}

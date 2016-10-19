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
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import publisher.debs2016.Debs2016Query1Publisher;
import publisher.debs2016.Debs2016Query2Publisher;
import publisher.schedular.PrimaryVMStartDecisionTaker;
import publisher.schedular.SecondaryVMStartDecisionTaker;
import publisher.schedular.util.Compressor;
import publisher.schedular.util.DataPublisherUtil;
import publisher.schedular.util.SwitchingConfigurations;
import publisher.schedular.vm.VMSimulator;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//mvn exec:java -Dexec.mainClass="publisher.ResearchEventPublisher"
public class ResearchEventPublisher{
    public static final int EMAIL_PROCESSOR_ID = 1;
    public static final int DEBS_Q1_ID = 2;
    public static final int VM_ID_PRIMARY = 1;
    public static final int VM_ID_SECOND = 2;

    private static Log log = LogFactory.getLog(ResearchEventPublisher.class);
    private static DataPublisher privateDataPublisher;
    private static DataPublisher currentDataPublisher;
    private static DataPublisher publicDataPublisher_1;
    private static DataPublisher publicDataPublisher_2;
    private static StreamDefinition streamDefinition;
    private static List<Object[]> events = new ArrayList<Object[]>();
    private static long startTime = System.currentTimeMillis();
    private static int count = 0;
    private static boolean sendToPublicCloud = false;
    private static int publicCloudPublishingRatio = 3; // Tells how much events to be published to public cloud for every 1000 events;
    private static int currentPublicPublishCount = 0;
    private static int privateSent = 0;
    private static int publicSent = 0;
    private static int totalPublicSent = 0;
    private static  ArrayList<DataPublisher> publicCloudPublishers = new ArrayList<DataPublisher>();


    //This thread runs the evaluation to decide  if we need to start a VM on public cloud
    private static PrimaryVMStartDecisionTaker vmDecisionTaker = new PrimaryVMStartDecisionTaker();
    private static Thread vmDecisionTakerThread = new Thread(vmDecisionTaker);

    private static SecondaryVMStartDecisionTaker secondaryVMStartDecisionTaker = new SecondaryVMStartDecisionTaker();
    private static Thread secondaryVmDecisionTakerThread = new Thread(secondaryVMStartDecisionTaker);

    // This thread runs the evaluation to decide if we need to send data to public cloud. This thread is run only when VM is started.
    private static DataPublishDecisionTaker dataPublishDecisionTaker = new DataPublishDecisionTaker();
    private static Thread dataPublishDecisionTakerThread = new Thread(dataPublishDecisionTaker);

    private static VMSimulator primaryVmSimulator = new VMSimulator(VM_ID_PRIMARY);
    private static VMSimulator secondaryVmSimulator = new VMSimulator(VM_ID_SECOND);

    private static boolean secondaryVmStarted = false;
    private static int publisherCount = 1;

    private static boolean isSwitching = false;

    private static long cumulativeMessageSize = 0;
    private static int tick = 0;

    public static void main(String[] args) throws InterruptedException {


        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        try {
            SwitchingConfigurations.setThresholdLatency(10 * 1000);
            SwitchingConfigurations.setThresholdThroughput(7500);
            SwitchingConfigurations.setTolerancePeriod(20 * 1000);
            SwitchingConfigurations.setVmStartDelay(10 * 1000);
            SwitchingConfigurations.setVmBillingSessionDuration(60*1000);
            SwitchingConfigurations.setPublicCloudEndpoint("192.168.57.79", 7611);
            SwitchingConfigurations.setMinEventsToKeepVm(100000);
            SwitchingConfigurations.setPublicCloudPublishThresholdLatency(12 * 1000);

            SwitchingConfigurations.setSecondaryVmStartupThreshold(21 * 1000);
            SwitchingConfigurations.setSecondaryVmDataPublishThreshold(22 * 1000);
            SwitchingConfigurations.setSecondaryVmStartupThresholdConsecutiveCount(2);

            System.out.println("Starting WSO2 Event ResearchEventPublisher Stream Client");
            AgentHolder.setConfigPath(DataPublisherUtil.filePath + "/src/main/java/files/configs/data-agent-config.xml");
            DataPublisherUtil.setTrustStoreParams();
            String protocol = "thrift";
            String singleNodeHost = "tcp://localhost:7611";
            String username = "admin";
            String password = "admin";

            privateDataPublisher = new DataPublisher(protocol, singleNodeHost , null, username, password);
            currentDataPublisher = privateDataPublisher;


            if (isSwitching) {
                //publicDataPublisher_1 = new DataPublisher(protocol, "tcp://192.168.1.7:7611", null, username, password);
                //publicCloudPublishers.add(publicDataPublisher_1);

               //publicDataPublisher_2 = new DataPublisher(protocol, "tcp://192.168.1.3:7611", null, username, password);
               //publicCloudPublishers.add(publicDataPublisher_2);
            }


            //Setting Threshold values for Switching
            //Publishable emailProcessorPublisher = new EmailBenchmarkPublisher();
            Publishable debs2016Query1Publisher = new Debs2016Query1Publisher();
            Publishable publisher = new Debs2016Query2Publisher();

            DataPublisherUtil.loadStreamDefinitions();

            if (isSwitching) {
                vmDecisionTaker.start();
                vmDecisionTakerThread.start();
                //secondaryVmDecisionTakerThread.start();
                dataPublishDecisionTakerThread.start();
            }

            //emailProcessorPublisher.startPublishing();
            //debs2016Query1Publisher.startPublishing();
            publisher.startPublishing();

            System.out.println("Public :" + publicSent);
            System.out.println("Private : " + privateSent);

        } catch (Throwable e) {

            log.error(e);
        }
    }

    public static Object[] compress(Object[] eventPayload){
        // For email processor
        try {
            eventPayload[2] = Compressor.compress(eventPayload[2].toString());
            eventPayload[3] = Compressor.compress(eventPayload[3].toString());
            eventPayload[4] = Compressor.compress(eventPayload[4].toString());
            eventPayload[6] = Compressor.compress(eventPayload[6].toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return eventPayload;
    }
    public static  void publishEvent(Object[] eventPayload, String streamId) throws InterruptedException {

        if (sendToPublicCloud && (currentDataPublisher == privateDataPublisher)){
            if (count % (100 - publicCloudPublishingRatio) == 0){
                currentDataPublisher = null; //setting to null for it to be picked interchangeably when  sending event in line # 162
                publicSent++;
            }
        }

        if (currentDataPublisher != privateDataPublisher){
            publicSent++;
            eventPayload = compress(eventPayload);
        } else {
            privateSent++;
        }

        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (currentDataPublisher != privateDataPublisher){
            currentDataPublisher = publicCloudPublishers.get((count % 10000) % publisherCount);
            //currentDataPublisher = publicCloudPublishers.get(0);
            currentDataPublisher.tryPublish(event);
        } else {
            currentDataPublisher.publish(event);
       }

        if (currentDataPublisher != privateDataPublisher){
            if (++currentPublicPublishCount == publicCloudPublishingRatio){
                currentDataPublisher = privateDataPublisher;
                currentPublicPublishCount = 0;
            }
        }

        if (++count % 50000 == 0) {
            Thread.sleep(1000);
        }

        if (count % 1000000 == 0){
            System.out.println("Done Sending " + count/1000000  + " Million Events");
        }
    }

    public static  void publishEvent(Object[] eventPayload, String streamId, int id) throws InterruptedException {

        if (sendToPublicCloud && (currentDataPublisher != publicDataPublisher_1) && (id == EMAIL_PROCESSOR_ID)){
            if (count % 100 == 0){
                currentDataPublisher = publicDataPublisher_1;
            }
        }

        if (currentDataPublisher == publicDataPublisher_1){
            publicSent++;
        } else {
            privateSent++;
        }

        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        currentDataPublisher.tryPublish(event);

        if (currentDataPublisher == publicDataPublisher_1){
            if (++currentPublicPublishCount == publicCloudPublishingRatio){
                currentDataPublisher = privateDataPublisher;
                currentPublicPublishCount = 0;
            }
        }

        count++;
    }

    public synchronized static void publishMultiplePublishers(Object[] eventPayload, String streamId, int id) throws InterruptedException {

       // publishEvent(eventPayload, streamId, id);

        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (sendToPublicCloud && id == DEBS_Q1_ID){
            publicDataPublisher_1.tryPublish(event);
        } else{
            privateDataPublisher.publish(event);
            //publishEvent(eventPayload, streamId, id);
        }
    }

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
    /**
     * Call back for PrimaryVMStartDecisionTaker to notify publisher to trigger start of VM
     */
    public static void StartVM(int id) {
        if (id == VM_ID_PRIMARY) {
            System.out.println("{" + new Date().toString() + "} - Primary VM Startup initiating");
            primaryVmSimulator.startVM();
            vmDecisionTaker.stop();
        } else {
            secondaryVmSimulator.startVM();
            secondaryVMStartDecisionTaker.stop();
            System.out.println("{" + new Date().toString() + "} - Secondary VM Startup initiating");
        }
    }

    /**
     * Call back for VMSimulator to notify start of VM
     */
    public static void OnVmStarted(int id){
        if (id == VM_ID_PRIMARY) {
            System.out.println("{" + new Date().toString() + "} - Primary VM Has started.");
            dataPublishDecisionTaker.start();
        } else {
            secondaryVmStarted = true;
            System.out.println("{" + new Date().toString() + "} - Secondary VM Has started.");
        }
    }

    /**
     * Callback for VMSimulator to notify Shutdown of VM
     */
    public static void OnVMSessionAboutToExpire(int id){
        totalPublicSent += publicSent;
        if (id == VM_ID_PRIMARY) {
            System.out.println("{" + new Date().toString() + "} - Primary VM is going to shutdown in a while");
            if (SwitchingConfigurations.getMinEventsToKeepVm() > publicSent &&
                    dataPublishDecisionTaker.getCurrentLatency() < SwitchingConfigurations.getVmStartTriggerThresholdLatency()) {
                // This is simulating VM shutdown.
                publicSent = 0; // Reset the public event sent count
                dataPublishDecisionTaker.stop(); // Stop decision thread to which evaluates if we need to publish data to public cloud
                sendToPublicCloud = false; // Stop publishing data to VM
                vmDecisionTaker.start(); // Start the VMStartDecision take thread to see if we need a VM again in the future.
                System.out.println("{" + new Date().toString() + "}[EVENT] - No enough events sent to public cloud. Shutting down the primary instance.");
            } else {
                primaryVmSimulator.keepTheVM();
                System.out.println("{" + new Date().toString() + "}[EVENT] -" + publicSent + " Events sent to public cloud. Keeping the VM instance.");
                publicSent = 0;

            }
        } else {
            if (dataPublishDecisionTaker.getCurrentLatency() < SwitchingConfigurations.getSecondaryVmStartupThreshold()){
                System.out.println("{" + new Date().toString() + "}[EVENT] - Latency is not high enough to keep the secondary instance. Shutting down the secondary instance.");
                secondaryVmStarted = false;
            } else {
                secondaryVmSimulator.keepTheVM();
                System.out.println("{" + new Date().toString() + "}[EVENT] - Latency is still high. Keeping the secondary VM instance.");
            }
        }
        System.out.println("Total Events Sent To public Cloud=" + totalPublicSent);
    }

    /**
     * Callback for DataPublisherDecisionTaker to notify start sending to public cloud
     */
    public static void StartSendingToPublicCloud(){
        sendToPublicCloud = true;
        if (secondaryVmStarted == false) {
            //secondaryVMStartDecisionTaker.start();
        }
    }

    /**
     * Callback for DataPublisherDecisionTaker to notify the stop of sending to public cloud
     */
    public static void StopSendingToPublicCloud(){
        sendToPublicCloud = false;
    }

    public static void SendToSecondaryInstance(){
        if (publisherCount != 2 && secondaryVmStarted){
            publisherCount = 2;
        }
    }

    public static void StopSendingToSecondaryInstance(){
        if (publisherCount != 2){
            publisherCount = 1;
        }
    }

    public static void addMessageSize(int messageSize){
        cumulativeMessageSize += messageSize;


        if (System.currentTimeMillis() - startTime >= 10000){
            startTime = System.currentTimeMillis();
            System.out.println(String.format(++tick * 10 + "," + new DecimalFormat("#.00").format(cumulativeMessageSize / (1024.0 * 1024.0 * 10))) );
            cumulativeMessageSize = 0;
        }
    }

}

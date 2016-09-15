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
import publisher.email.EmailBenchmarkPublisher;
import publisher.schedular.VMStartDecisionTaker;
import publisher.schedular.util.DataPublisherUtil;
import publisher.schedular.util.SwitchingConfigurations;
import publisher.schedular.vm.VMSimulator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//mvn exec:java -Dexec.mainClass="publisher.ResearchEventPublisher"
public class ResearchEventPublisher{
    public static final int EMAIL_PROCESSOR_ID = 1;
    public static final int DEBS_Q1_ID = 2;

    private static Log log = LogFactory.getLog(ResearchEventPublisher.class);
    private static DataPublisher privateDataPublisher;
    private static DataPublisher currentDataPublisher;
    private static DataPublisher publicDataPublisher;
    private static StreamDefinition streamDefinition;
    private static List<Object[]> events = new ArrayList<Object[]>();
    private static long startTime = System.currentTimeMillis();
    private static int count = 0;
    private static boolean sendToPublicCloud = false;
    private static int publicCloudPublishingRatio = 3; // Tells how much events to be published to public cloud for every 10 events;
    private static int currentPublicPublishCount = 0;
    private static int privateSent = 0;
    private static int publicSent = 0;


    //This thread runs the evaluation to decide  if we need to start a VM on public cloud
    private static VMStartDecisionTaker vmDecisionTaker = new VMStartDecisionTaker();
    private static Thread vmDecisionTakerThread = new Thread(vmDecisionTaker);

    // This thread runs the evaluation to decide if we need to send data to public cloud. This thread is run only when VM is started.
    private static DataPublishDecisionTaker dataPublishDecisionTaker = new DataPublishDecisionTaker();
    private static Thread dataPublishDecisionTakerThread = new Thread(dataPublishDecisionTaker);

    private static VMSimulator vmSimulator = new VMSimulator();

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
            SwitchingConfigurations.setPublicCloudEndpoint("192.168.1.4", 7611);
            SwitchingConfigurations.setMinEventsToKeepVm(100000);
            SwitchingConfigurations.setPublicCloudPublishThresholdLatency(12 * 1000);

            System.out.println("Starting WSO2 Event ResearchEventPublisher Stream CLient");
            AgentHolder.setConfigPath(DataPublisherUtil.filePath + "/src/main/java/files/configs/data-agent-config.xml");
            DataPublisherUtil.setTrustStoreParams();
            String protocol = "thrift";
            String singleNodeHost = "tcp://localhost:7611";
            String username = "admin";
            String password = "admin";

            privateDataPublisher = new DataPublisher(protocol,  singleNodeHost , null, username, password);
            currentDataPublisher = privateDataPublisher;

            publicDataPublisher = new DataPublisher(protocol, "tcp://" + SwitchingConfigurations.getPublicCloudEndpoint().toString(), null, username, password);


            //Setting Threshold values for Switching
            Publishable emailProcessorPublisher = new EmailBenchmarkPublisher();
            Publishable debs2016Query1Publisher = new Debs2016Query1Publisher();
            //Publishable publisher = new Debs2016Query2Publisher();

            DataPublisherUtil.loadStreamDefinitions();


            vmDecisionTaker.start();
            vmDecisionTakerThread.start();
            dataPublishDecisionTakerThread.start();

            emailProcessorPublisher.startPublishing();
            //debs2016Query1Publisher.startPublishing();

            System.out.println("Public :" + publicSent);
            System.out.println("Private : " + privateSent);
        } catch (Throwable e) {

            log.error(e);
        }
    }

    public static  void publishEvent(Object[] eventPayload, String streamId) throws InterruptedException {

        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (sendToPublicCloud && (currentDataPublisher != publicDataPublisher)){
            if (count % 100 == 0){
                currentDataPublisher = publicDataPublisher;
                publicSent++;
            }
        }

        if (currentDataPublisher == publicDataPublisher){
            publicSent++;
        } else {
            privateSent++;
        }

        currentDataPublisher.tryPublish(event);

        if (currentDataPublisher == publicDataPublisher){
            if (++currentPublicPublishCount == publicCloudPublishingRatio){
                currentDataPublisher = privateDataPublisher;
                currentPublicPublishCount = 0;
            }
        }

        if (++count % 12000 == 0) {
            Thread.sleep(1000);
            //System.out.println(count + " Events published in " + (System.currentTimeMillis() - startTime) + "ms");
            //System.out.println("Public :" + publicSent);
            //System.out.println("Private : " + privateSent);
            startTime = System.currentTimeMillis();
        }
    }

    public static  void publishEvent(Object[] eventPayload, String streamId, int id) throws InterruptedException {

        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (sendToPublicCloud && (currentDataPublisher != publicDataPublisher) && (id == EMAIL_PROCESSOR_ID)){
            if (count % 100 == 0){
                currentDataPublisher = publicDataPublisher;
            }
        }

        if (currentDataPublisher == publicDataPublisher){
            publicSent++;
        } else {
            privateSent++;
        }

        currentDataPublisher.tryPublish(event);

        if (currentDataPublisher == publicDataPublisher){
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
            publicDataPublisher.tryPublish(event);
        } else{
            publishEvent(eventPayload, streamId);
        }
    }

    public static void sendOutofOrder(Object[] eventPayload, String streamId, boolean isOutOfOrder) throws InterruptedException {
        Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);

        if (!isOutOfOrder && sendToPublicCloud){
            publishEvent(eventPayload, streamId);
        } else{
            privateDataPublisher.tryPublish(event);
        }

        if (++count % 12000 == 0) {
            Thread.sleep(1000);
        }
    }
    /**
     * Call back for VMStartDecisionTaker to notify publisher to trigger start of VM
     */
    public static void StartVM() {
        System.out.println("{" + new Date().toString() + "} - VM Startup initiating");
        vmSimulator.startVM();
        vmDecisionTaker.stop();
    }

    /**
     * Call back for VMSimulator to notify start of VM
     */
    public static void OnVmStarted(){
        System.out.println("{" + new Date().toString() + "} - VM Has started.");
        dataPublishDecisionTaker.start();
    }

    /**
     * Callback for VMSimulator to notify Shutdown of VM
     */
    public static void OnVMSessionAboutToExpire(){
        System.out.println("{" + new Date().toString() + "} - VM is going to shutdown in a while");
        if (SwitchingConfigurations.getMinEventsToKeepVm() > publicSent &&
                dataPublishDecisionTaker.getCurrentLatency() < SwitchingConfigurations.getVmStartTriggerThresholdLatency()){
            // This is simulating VM shutdown.
            publicSent = 0; // Reset the public event sent count
            dataPublishDecisionTaker.stop(); // Stop decision thread to which evaluates if we need to publish data to public cloud
            sendToPublicCloud = false; // Stop publishing data to VM
            vmDecisionTaker.start(); // Start the VMStartDecision take thread to see if we need a VM again in the future.
            System.out.println("{" + new Date().toString() + "}[EVENT] - No enough events sent to public cloud. Shutting down the instance.");
        } else {
            vmSimulator.keepTheVM();
            System.out.println("{" + new Date().toString() + "}[EVENT] -" + publicSent + " Events sent to public cloud. Keeping the VM instance.");
            publicSent = 0;

        }
    }

    /**
     * Callback for DataPublisherDecisionTaker to notify start sending to public cloud
     */
    public static void StartSendingToPublicCloud(){
        sendToPublicCloud = true;
    }

    /**
     * Callback for DataPublisherDecisionTaker to notify the stop of sending to public cloud
     */
    public static void StopSendingToPublicCloud(){
        sendToPublicCloud = false;
    }
}

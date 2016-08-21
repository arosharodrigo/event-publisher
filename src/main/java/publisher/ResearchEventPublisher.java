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
import publisher.email.EmailBenchmarkPublisher;
import publisher.schedular.VMStartDecisionTaker;
import publisher.schedular.util.DataPublisherUtil;
import publisher.schedular.util.SwitchingConfigurations;
import publisher.schedular.vm.VMSimulator;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

//mvn exec:java -Dexec.mainClass="publisher.ResearchEventPublisher"
public class ResearchEventPublisher{
    private static Log log = LogFactory.getLog(ResearchEventPublisher.class);
    private static DataPublisher privateDataPublisher;
    private static DataPublisher currentDataPublisher;
    private static DataPublisher publicDataPublisher;
    private static StreamDefinition streamDefinition;
    private static List<Object[]> events = new ArrayList<Object[]>();
    private static long startTime = System.currentTimeMillis();
    private static int count = 0;
    private static boolean sendToPublicCloud = false;
    private static int publicCloudPublishingRatio = 5; // Tells how much events to be published to public cloud for every 10 events;
    private static int currentPublicPublishCount = 0;
    private static int privateSent = 0;
    private static int publicSent = 0;

    //This thread runs the evaluation to decide  if we need to start a VM on public cloud
    private static Thread vmDecisionTakerThread = new Thread(new VMStartDecisionTaker());

    // This thread runs the evaluation to decide if we need to send data to public cloud. This thread is run only when VM is started.
    private static Thread dataPublishDecisionTakerThread = new Thread(new DataPublishDecisionTaker());

    private static VMSimulator vmSimulator = new VMSimulator();

    public static void main(String[] args) throws InterruptedException {


        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        try {

            NetworkInterface.getNetworkInterfaces();
            InetAddress.getLocalHost();

            SwitchingConfigurations.setGetThresholdLatency(20 * 1000);
            SwitchingConfigurations.setThresholdThroughput(7500);
            SwitchingConfigurations.setTolerancePeriod(40 * 1000);
            SwitchingConfigurations.setVmStartDelay(10 * 1000);
            SwitchingConfigurations.setVmBillingSessionDuration(30*1000);
            SwitchingConfigurations.setPublicCloudEndpoint("192.168.1.5", 7611);

            System.out.println("Starting WSO2 Event ResearchEventPublisher Stream CLient");
            AgentHolder.setConfigPath(DataPublisherUtil.filePath + "/src/main/java/files/configs/data-agent-config.xml");
            DataPublisherUtil.setTrustStoreParams();
            String protocol = "thrift";
            String singleNodeHost = "tcp://localhost:7611";
            String username = "admin";
            String password = "admin";

            privateDataPublisher = new DataPublisher(protocol,  singleNodeHost , null, username, password);
            currentDataPublisher = privateDataPublisher;

            //publicDataPublisher = new DataPublisher(protocol, "tcp://" + SwitchingConfigurations.getPublicCloudEndpoint().toString(), null, username, password);


            //Setting Threshold values for Switching
            Publishable publisher = new EmailBenchmarkPublisher();
            //Publishable publisher = new Debs2016Query1Publisher();
            //Publishable publisher = new Debs2016Query2Publisher();

            Map<String, StreamDefinition> streamDefinitions = DataPublisherUtil.loadStreamDefinitions();
            streamDefinition = streamDefinitions.get(publisher.getStreamId());

            if (streamDefinition == null) {
                throw new Exception("StreamDefinition not available for stream " + publisher.getStreamId());
            } else {
                log.info("StreamDefinition used :" + streamDefinition);
            }


            vmDecisionTakerThread.start();

            publisher.startPublishing();
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

    /**
     * Call back for VMStartDecisionTaker to notify publisher to trigger start of VM
     */
    public static void StartVM() {
        System.out.println("Starting the at " + new Date().toString());
        vmSimulator.startVM();
        vmDecisionTakerThread.stop();
    }

    /**
     * Call back for VMSimulator to notify start of VM
     */
    public static void OnVmStarted(){
        System.out.println("VM Has started at " + new Date().toString());
        dataPublishDecisionTakerThread.start();
    }

    /**
     * Callback for VMSimulator to notify Shutdown of VM
     */
    public static void OnVMSessionAboutToExpire(){
        System.out.println("VM is going to shutdown in a while at " + new Date().toString());
        if (SwitchingConfigurations.getMinEventsToKeepVm() < publicSent){
            // This is simulating VM shutdown.
            publicSent = 0; // Reset the public event sent count
            dataPublishDecisionTakerThread.stop(); // Stop decision thread to which evaluates if we need to publish data to public cloud
            sendToPublicCloud = false; // Stop publishing data to VM
            vmDecisionTakerThread.start(); // Start the VMStartDecision take thread to see if we need a VM again in the future.
        }
    }

    /**
     * Callback for DataPublisherDecisionTaker to notify start sending to public cloud
     */
    public static void StartSendingToPublicCloud(){
        System.out.println("Start Sending to public Cloud");
        sendToPublicCloud = true;
    }

    /**
     * Callback for DataPublisherDecisionTaker to notify the stop of sending to public cloud
     */
    public static void StopSendingToPublicCloud(){
        System.out.println("Stop Sending to public Cloud");
        sendToPublicCloud = false;
    }
}

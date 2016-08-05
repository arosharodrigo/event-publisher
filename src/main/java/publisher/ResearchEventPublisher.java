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
import publisher.schedular.DecisionTaker;
import publisher.schedular.SwitchingConfigurations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//mvn exec:java -Dexec.mainClass="publisher.ResearchEventPublisher"
public class ResearchEventPublisher{
    private static Log log = LogFactory.getLog(ResearchEventPublisher.class);
    private static DataPublisher singleNodeDataPublisher;
    private static DataPublisher loadBalancedDataPublisher;
    private static DataPublisher currentDataPublisher;
    private static StreamDefinition streamDefinition;
    private static List<Object[]> events = new ArrayList<Object[]>();
    private static long startTime = System.currentTimeMillis();
    private static int count = 0;
    private static Lock dataPublisherLock = new ReentrantLock();

    public static void main(String[] args) throws InterruptedException {

        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        try {

            SwitchingConfigurations.setGetThresholdLatency(10 * 1000);
            SwitchingConfigurations.setThresholdThroughput(3500);
            SwitchingConfigurations.setPublicCloudEndpoint("127.0.0.1", 7612);

            System.out.println("Starting WSO2 Event ResearchEventPublisher Stream CLient");
            AgentHolder.setConfigPath(DataPublisherUtil.filePath + "/src/main/java/files/configs/data-agent-config.xml");
            DataPublisherUtil.setTrustStoreParams();
            String protocol = "thrift";
            String singleNodeHost = "tcp://127.0.0.1:7611";
            String username = "admin";
            String password = "admin";

            singleNodeDataPublisher = new DataPublisher(protocol,  singleNodeHost , null, username, password);
            currentDataPublisher = singleNodeDataPublisher;

            loadBalancedDataPublisher = new DataPublisher(protocol,
                    singleNodeHost + ", tcp://" + SwitchingConfigurations.getPublicCloudEndpoint().toString(), null, username, password);


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

            Thread decisionTakerThread = new Thread(new DecisionTaker());
            decisionTakerThread.start();

            publisher.startPublishing();
        } catch (Throwable e) {

            log.error(e);
        }
    }

    public static  void publishEvent(Object[] eventPayload, String streamId) throws InterruptedException {
            //System.out.println(Arrays.deepToString(eventPayload));
            //System.out.println(currentDataPublisher);
            Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);
            dataPublisherLock.lock();
            currentDataPublisher.publish(event);
            dataPublisherLock.unlock();

            if (++count % 5000 == 0) {
                Thread.sleep(1000);
                System.out.println(count + " Events published in " + (System.currentTimeMillis() - startTime) + "ms");
                startTime = System.currentTimeMillis();
            }
    }


    public static void Switch() {
        dataPublisherLock.lock();
        currentDataPublisher = loadBalancedDataPublisher;
        dataPublisherLock.unlock();
        System.out.println("We need to switch to public cloud.......");
    }
}

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

package wso2event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import wso2event.email.EmailBenchmarkPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//mvn exec:java -Dexec.mainClass="wso2event.ResearchEventPublisher"
public class ResearchEventPublisher {
    private static Log log = LogFactory.getLog(ResearchEventPublisher.class);
    private static DataPublisher dataPublisher;
    private static StreamDefinition streamDefinition;
    private static List<Object[]> events = new ArrayList<Object[]>();
    private static long startTime = System.currentTimeMillis();
    private static int count = 0;

    public static void main(String[] args) throws InterruptedException {

        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        try {

            System.out.println("Starting WSO2 Event ResearchEventPublisher Stream CLient");
            AgentHolder.setConfigPath(DataPublisherUtil.filePath + "/src/main/java/files/configs/data-agent-config.xml");
            DataPublisherUtil.setTrustStoreParams();
            String protocol = "thrift";
            String host = "127.0.0.1";
            String port = "7611";
            String username = "admin";
            String password = "admin";
            dataPublisher = new DataPublisher(protocol, "tcp://" + host + ":" + port, null, username, password);

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

            publisher.startPublishing();
        } catch (Throwable e) {

            log.error(e);
        }
    }

    public static  void publishEvent(Object[] eventPayload, String streamId) throws InterruptedException {
           //System.out.println(Arrays.deepToString(eventPayload));
            Event event = new Event(streamId, System.currentTimeMillis(), null, null, eventPayload);
            dataPublisher.publish(event);

            if (++count % 5000 == 0) {
                Thread.sleep(1000);
                System.out.println(count + " Events published in " + (System.currentTimeMillis() - startTime) + "ms");
                startTime = System.currentTimeMillis();
            }
    }


}

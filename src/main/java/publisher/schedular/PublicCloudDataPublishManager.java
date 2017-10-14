package publisher.schedular;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import publisher.DataPublishDecisionTaker;
import publisher.ResearchEventPublisher;
import publisher.schedular.util.Configurations;
import publisher.schedular.util.StatisticsInputReaderTask;
import publisher.schedular.vm.VMConfig;
import publisher.util.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sajith on 11/27/16.
 */
public class PublicCloudDataPublishManager implements DataPublishDecisionListener {
    private static Log log = LogFactory.getLog(PublicCloudDataPublishManager.class);

    Map<Integer, DataPublishDecisionTaker> dataPublishDecisionTakers = new HashMap<>();
    public static Map<Integer, VMConfig> vmIdToConfigurations = new HashMap<>();
    public static Map<Integer, DataPublisher> vmIdToDataPublisher = new ConcurrentHashMap<>();

    /**
     * Method for the main class to register a VM with the data publish manager. Once registered a VM the manager will
     * spawn a separate decision take for the VM and start firing call backs to the Main class to send and stop sending events
     * to the particular VM
     * @param vmConfig
     */
    public void registerVM(VMConfig vmConfig){

        DataPublishDecisionTaker dataPublishDecisionTaker = dataPublishDecisionTakers.get(vmConfig.getVmId());
        if (dataPublishDecisionTaker == null) {
            dataPublishDecisionTaker = new DataPublishDecisionTaker(vmConfig.getVmId(), vmConfig.getDataPublishThresholdLatency(), this);
            dataPublishDecisionTakers.put(vmConfig.getVmId(), dataPublishDecisionTaker);
            new Thread(dataPublishDecisionTaker).start();
        }

        dataPublishDecisionTaker.start();
        vmIdToConfigurations.put(vmConfig.getVmId(), vmConfig);

        log.debug("VM Registered[ID=" + vmConfig.getVmId() + "]");
    }

    /**
     * Method for main class to notify that a VM is going to shutdown. This methods evaluvates if it's worthwhile to
     * retain the VM notifies the decision to the main class.
     * @param vmConfig Configuration of the VM
     * @param eventsSent number of events that was sent during last session to the VM
     * @return true if the VM should retain. false otherwise.
     */
    public boolean vmGoingToShutDown(VMConfig vmConfig, long eventsSent){
        DataPublishDecisionTaker decisionTaker = dataPublishDecisionTakers.get(vmConfig.getVmId());
        boolean isLatencyRecovered = decisionTaker.isLatencyRecovered(); // if latency is not recovered keep the VM

        boolean keepTheVM = true;
        if (isLatencyRecovered && eventsSent < Configurations.getMinEventsToKeepVm()){
            keepTheVM = false;
            log.info("VM Shutting Down. No retaining [ID=" + vmConfig.getVmId()
                    + ", isLatencyRecovered=" + isLatencyRecovered
                    + ", EventsSentToPublicCloud=" + eventsSent
                    + ", ThresholdEventCountToRetain=" + Configurations.getMinEventsToKeepVm() +"]");

            // Stop the decision taker as the VM is no longer running
            decisionTaker.stop();

            try {
                DataPublisher dataPublisher = vmIdToDataPublisher.get(vmConfig.getVmId());
                if (dataPublisher != null){ // Data publisher can be null if the VM started and the data is not published as the latency is recovered
                    dataPublisher.shutdown();
                }
                vmIdToDataPublisher.remove(vmConfig.getVmId());
            } catch (DataEndpointException e) {
                log.error("Error while trying to shutdown data Publisher " + e.getMessage(), e);
            }
        }

        return keepTheVM;
    }

    /**
     * Callback form decision takers to notify that now events should be sent to the VM with vmId
     */
    @Override
    public synchronized void SendDataToVm(int vmId) {
        VMConfig vmConfig = vmIdToConfigurations.get(vmId);
        try {
            DataPublisher dataPublisher = vmIdToDataPublisher.get(vmId);
            if (dataPublisher == null) {
                dataPublisher = new DataPublisher(Configuration.getProperty("protocol"),
                        vmConfig.getThriftUrl(), null, ResearchEventPublisher.USER_NAME, ResearchEventPublisher.PASSWORD);
                vmIdToDataPublisher.put(vmConfig.getVmId(), dataPublisher);
            }

            ResearchEventPublisher.onSendDataToVM(dataPublisher);
        } catch (Exception e) {
            log.error("Error while trying to connect to VM" +  e.getMessage(), e);
        }

        log.debug("Starting to send data to VM[ID=" + vmId + ", URL=" + vmConfig.getThriftUrl() + "]");
    }

    /**
     * Callback form decision takers to notify that now events should not be sent to the VM with vmId
     */
    @Override
    public synchronized void StopSendingDataToVm(int vmId) {
        DataPublisher dataPublisher = vmIdToDataPublisher.get(vmId);
        ResearchEventPublisher.onStopSendingDataToVM(dataPublisher);
        log.debug("Stopping sending data to VM[ID=" + vmId + "]");
    }

    public static DataPublisher generateDataPublisher(int vmId) throws Exception {
        VMConfig vmConfig = vmIdToConfigurations.get(vmId);
        return new DataPublisher(Configuration.getProperty("protocol"),
                vmConfig.getThriftUrl(), null, ResearchEventPublisher.USER_NAME, ResearchEventPublisher.PASSWORD);
    }
}

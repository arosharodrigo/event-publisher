package publisher.schedular.vm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import publisher.ResearchEventPublisher;
import publisher.schedular.VMStartDecisionTaker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sajith on 11/26/16.
 */
public class VMManager implements VMStartDecisionListener, VMEventListener {
    private static Log log = LogFactory.getLog(VMManager.class);

    List<VMConfig> vmConfigurationList;
    Map<Integer, VMStartDecisionTaker> vmStartDecisionTakers = new HashMap<>();
    Map<Integer, VMSimulator> vmSimulators = new HashMap<>();
    Map<Integer, VMConfig> vmIdToConfigurations = new HashMap<>();

    public VMManager(List<VMConfig> vmConfigurationList){
        this.vmConfigurationList = vmConfigurationList;
    }

    public void start(){
        log.info("Starting VM Manager with " + vmConfigurationList.size() + "VMs");
        for (VMConfig vmConfig : vmConfigurationList){

            VMStartDecisionTaker vmStartDecisionTaker = new VMStartDecisionTaker(vmConfig.getStartThresholdLatency(),
                    vmConfig.getVmId(), vmConfig.getTolerancePeriod(), this);
            vmStartDecisionTakers.put(vmConfig.getVmId(), vmStartDecisionTaker);
            System.out.println(vmConfig.getThriftUrl());
            vmIdToConfigurations.put(vmConfig.getVmId(), vmConfig);

            vmStartDecisionTaker.start();
            new Thread(vmStartDecisionTaker).start();
        }
    }

    /**
     * The callback given by the VMStartDecisionTaker taker to notify when it should initiate the starting up of the VM
     * @param id VM ID
     */
    @Override
    public synchronized void onTriggerVMStartUp(int id) {
        VMSimulator vm = new VMSimulator(id, this);
        vmSimulators.put(id, vm);
        vm.startVM();

        // Stop the VM startup decision taker as the VM is already running and not require to run VMStartDecisionMaker as the decision is being made
        vmStartDecisionTakers.get(id).stop();
        log.info("VM Startup Triggered[ID=" + id + "]");
    }

    /**
     * Callback from the VMSimulator to notify that that VM has started
     * @param id VM ID
     */
    @Override
    public synchronized void OnVMStarted(int id) {
        VMConfig vmConfig = vmIdToConfigurations.get(id);
        ResearchEventPublisher.OnVmStarted(vmConfig);
        log.info("VM Started[ID=" + id + "]");
    }

    /**
     * Callback from the VMSimulator to notify that that VMs billing period is ending
     * @param id VM ID
     */
    @Override
    public synchronized void OnVMBillingPeriodEnding(int id) {

        VMConfig vmConfig = vmIdToConfigurations.get(id);
        boolean keepTheVM = ResearchEventPublisher.OnVmGoingToShutDown(vmConfig);

        if (keepTheVM){
            vmSimulators.get(id).keepTheVM();
            log.info("VM Billing Period Ended. Retaining the VM[ID=" + id + "]");
        } else {
            log.info("VM Billing Period Ended. Stopping VM[ID=" + id + "]");
            vmSimulators.remove(id);
            vmStartDecisionTakers.get(id).start();
        }

    }
}

package publisher.schedular.vm;

import publisher.ResearchEventPublisher;
import publisher.schedular.util.SwitchingConfigurations;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by sajith on 8/17/16.
 */
public class VMSimulator {
    Timer vmStartTimer =  new Timer();
    Timer vmSessionTimer = new Timer();

    public void startVM(){
        vmStartTimer.schedule(new VMStartDelayTimerTask(),  SwitchingConfigurations.getVmStartDelay());
    }

    class VMStartDelayTimerTask extends TimerTask{

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
            System.out.println("VM has started....");
            ResearchEventPublisher.OnVmStarted();
            vmSessionTimer.schedule(new VMSessionTimerTask(), SwitchingConfigurations.getVmBillingSessionDuration());
        }
    }

    class VMSessionTimerTask extends TimerTask{

        @Override
        public void run() {
            System.out.println("VM is ready to shutdown.");
            ResearchEventPublisher.OnVMSessionAboutToExpire();
        }
    }
}

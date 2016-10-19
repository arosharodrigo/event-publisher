package publisher.schedular.vm;

import publisher.ResearchEventPublisher;
import publisher.schedular.util.SwitchingConfigurations;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by sajith on 8/17/16.
 */
public class VMSimulator {
    Timer vmStartTimer =  new Timer();
    Timer vmSessionTimer = new Timer();
    private int id;

    public VMSimulator(int id){
        this.id = id;
    }

    public void startVM(){
        vmStartTimer.schedule(new VMStartDelayTimerTask(),  SwitchingConfigurations.getVmStartDelay());
    }

    public void keepTheVM(){
        vmSessionTimer.schedule(new VMSessionTimerTask(), SwitchingConfigurations.getVmBillingSessionDuration());
    }

    class VMStartDelayTimerTask extends TimerTask{

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
            System.out.println("{" + new Date().toString() + "}[EVENT] - VM has started");
            ResearchEventPublisher.OnVmStarted(id);
            vmSessionTimer.schedule(new VMSessionTimerTask(), SwitchingConfigurations.getVmBillingSessionDuration());
        }
    }

    class VMSessionTimerTask extends TimerTask{

        @Override
        public void run() {
            System.out.println("{" + new Date().toString() + "}[EVENT] - VM is ready to shutdown.");
            ResearchEventPublisher.OnVMSessionAboutToExpire(id);
        }
    }
}

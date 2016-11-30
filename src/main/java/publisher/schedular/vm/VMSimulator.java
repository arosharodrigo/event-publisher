package publisher.schedular.vm;

import publisher.schedular.util.Configurations;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by sajith on 8/17/16.
 */
public class VMSimulator {
    Timer vmStartTimer =  new Timer();
    Timer vmSessionTimer = new Timer();
    private int id;
    VMEventListener listener;

    public VMSimulator(int id, VMEventListener listener){
        this.id = id;
        this.listener = listener;
    }

    public void startVM(){
        vmStartTimer.schedule(new VMStartDelayTimerTask(),  Configurations.getVmStartDelay());
    }

    public void keepTheVM(){
        vmSessionTimer.schedule(new VMSessionTimerTask(), Configurations.getVmBillingSessionDuration());
    }

    class VMStartDelayTimerTask extends TimerTask{

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
            listener.OnVMStarted(id);
            vmSessionTimer.schedule(new VMSessionTimerTask(), Configurations.getVmBillingSessionDuration());
        }
    }

    class VMSessionTimerTask extends TimerTask{

        @Override
        public void run() {
            listener.OnVMBillingPeriodEnding(id);
        }
    }
}

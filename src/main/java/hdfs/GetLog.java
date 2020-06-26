package hdfs;


import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Create by GuoJF on 2019/5/19
 */
public class GetLog {
    public static void main(String[] args) throws SchedulerException {



        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        QuartzManager quartzManager = new QuartzManager();

        quartzManager.setScheduler(scheduler);

        quartzManager.addJob("testJob01", "Group01", "Group01", "Group01", LogJob.class, "30 */1 * * * ?");



    }


}

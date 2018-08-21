package com.github.ltsopensource.jobtracker;

import com.github.ltsopensource.biz.logger.SmartJobLogger;
import com.github.ltsopensource.core.cluster.AbstractServerNode;
import com.github.ltsopensource.core.spi.ServiceLoader;
import com.github.ltsopensource.jobtracker.channel.ChannelManager;
import com.github.ltsopensource.jobtracker.cmd.AddJobHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.LoadJobHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.TriggerJobManuallyHttpCmd;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.domain.JobTrackerNode;
import com.github.ltsopensource.jobtracker.monitor.JobTrackerMStatReporter;
import com.github.ltsopensource.jobtracker.processor.RemotingDispatcher;
import com.github.ltsopensource.jobtracker.sender.JobSender;
import com.github.ltsopensource.jobtracker.support.JobReceiver;
import com.github.ltsopensource.jobtracker.support.NonRelyOnPrevCycleJobScheduler;
import com.github.ltsopensource.jobtracker.support.OldDataHandler;
import com.github.ltsopensource.jobtracker.support.checker.ExecutableDeadJobChecker;
import com.github.ltsopensource.jobtracker.support.checker.ExecutingDeadJobChecker;
import com.github.ltsopensource.jobtracker.support.checker.FeedbackJobSendChecker;
import com.github.ltsopensource.jobtracker.support.cluster.JobClientManager;
import com.github.ltsopensource.jobtracker.support.cluster.TaskTrackerManager;
import com.github.ltsopensource.jobtracker.support.listener.JobNodeChangeListener;
import com.github.ltsopensource.jobtracker.support.listener.JobTrackerMasterChangeListener;
import com.github.ltsopensource.jobtracker.support.policy.OldDataDeletePolicy;
import com.github.ltsopensource.queue.JobQueueFactory;
import com.github.ltsopensource.remoting.RemotingProcessor;

/**
 * @author Robert HG (254963746@qq.com) on 7/23/14.
 */
public class JobTracker extends AbstractServerNode<JobTrackerNode, JobTrackerAppContext> {

    public JobTracker() {
        // 添加节点变化监听器
        addNodeChangeListener(new JobNodeChangeListener(appContext));
        // 添加master节点变化监听器
        addMasterChangeListener(new JobTrackerMasterChangeListener(appContext));
    }

    @Override
    protected void beforeStart() {
        // 监控中心
        appContext.setMStatReporter(new JobTrackerMStatReporter(appContext));
        // channel 管理者
        appContext.setChannelManager(new ChannelManager());
        // JobClient 管理者
        appContext.setJobClientManager(new JobClientManager(appContext));
        // TaskTracker 管理者
        appContext.setTaskTrackerManager(new TaskTrackerManager(appContext));

        // injectRemotingServer
        appContext.setRemotingServer(remotingServer);
        appContext.setJobLogger(new SmartJobLogger(appContext));

        //两种Job队列管理, mysql和mongodb. 默认为mysql
        JobQueueFactory factory = ServiceLoader.load(JobQueueFactory.class, config);

        //lts_wjq_<tasktracker.node-group>, taskTracker每个nodeGroup对应一张表
        appContext.setExecutableJobQueue(factory.getExecutableJobQueue(config));

        //lts_executing_job_queue
        appContext.setExecutingJobQueue(factory.getExecutingJobQueue(config));

        //lts_cron_job_queue
        appContext.setCronJobQueue(factory.getCronJobQueue(config));

        //lts_repeat_job_queue
        appContext.setRepeatJobQueue(factory.getRepeatJobQueue(config));

        //lts_suspend_job_queue
        appContext.setSuspendJobQueue(factory.getSuspendJobQueue(config));

        //lts_fjq_<jobclient.node-group>, jobClient每个nodeGroup对应一张表
        appContext.setJobFeedbackQueue(factory.getJobFeedbackQueue(config));

        //lts_node_group_store,节点组信息表
        appContext.setNodeGroupStore(factory.getNodeGroupStore(config));

        //操作lts_wjq_<tasktracker.node-group>
        appContext.setPreLoader(factory.getPreLoader(appContext));

        //Job接收器, 从请求中将job放入对应类型的任务队列表中(cron,repeat)和lts_wjq_<tasktracker.node-group>
        appContext.setJobReceiver(new JobReceiver(appContext));

        //Job发送器, 从lts_wjq_<tasktracker.node-group>取出任务并发送
        appContext.setJobSender(new JobSender(appContext));

        //针对cron任务和repeat任务, 将其加入可执行列表
        appContext.setNonRelyOnPrevCycleJobScheduler(new NonRelyOnPrevCycleJobScheduler(appContext));

        //将可执行队列中正在执行(is_running==true) 且已超过1分钟的任务is_running修改为false
        appContext.setExecutableDeadJobChecker(new ExecutableDeadJobChecker(appContext));
        appContext.setExecutingDeadJobChecker(new ExecutingDeadJobChecker(appContext));
        appContext.setFeedbackJobSendChecker(new FeedbackJobSendChecker(appContext));


        //注册HTTP命令
        appContext.getHttpCmdServer().registerCommands(
                new LoadJobHttpCmd(appContext),     // 手动加载任务
                new AddJobHttpCmd(appContext),
                new TriggerJobManuallyHttpCmd(appContext));     // 添加任务

        if (appContext.getOldDataHandler() == null) {
            appContext.setOldDataHandler(new OldDataDeletePolicy());
        }
    }

    @Override
    protected void afterStart() {
        appContext.getChannelManager().start();
        appContext.getMStatReporter().start();
    }

    @Override
    protected void afterStop() {
        appContext.getChannelManager().stop();
        appContext.getMStatReporter().stop();
        appContext.getHttpCmdServer().stop();
    }

    @Override
    protected void beforeStop() {
    }

    @Override
    protected RemotingProcessor getDefaultProcessor() {
        return new RemotingDispatcher(appContext);
    }

    public void setOldDataHandler(OldDataHandler oldDataHandler) {
        appContext.setOldDataHandler(oldDataHandler);
    }

}

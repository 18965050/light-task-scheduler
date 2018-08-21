package com.github.ltsopensource.tasktracker.support;

import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.constant.EcTopic;
import com.github.ltsopensource.core.constant.ExtConfig;
import com.github.ltsopensource.core.exception.JobTrackerNotFoundException;
import com.github.ltsopensource.core.factory.NamedThreadFactory;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.protocol.JobProtos;
import com.github.ltsopensource.core.protocol.command.JobPullRequest;
import com.github.ltsopensource.ec.EventInfo;
import com.github.ltsopensource.ec.EventSubscriber;
import com.github.ltsopensource.ec.Observer;
import com.github.ltsopensource.jvmmonitor.JVMConstants;
import com.github.ltsopensource.jvmmonitor.JVMMonitor;
import com.github.ltsopensource.remoting.exception.RemotingCommandFieldCheckException;
import com.github.ltsopensource.remoting.protocol.RemotingCommand;
import com.github.ltsopensource.tasktracker.domain.TaskTrackerAppContext;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用来向JobTracker去取任务
 * 1. 会订阅JobTracker的可用,不可用消息主题的订阅
 * 2. 只有当JobTracker可用的时候才会去Pull任务
 * 3. Pull只是会给JobTracker发送一个通知
 *
 * @author Robert HG (254963746@qq.com) on 3/25/15.
 */


/**
 * <pre>
 *     触发条件:
 *     1. JobTracker可用
 *     2. 如果设置资源检查, 资源检查通过
 *     3. 任务运行线程池有空闲线程可运行任务
 *
 *     流程:
 *     1. TaskTracker发送JOB_PULL命令给JobTracker, 进行任务拉取
 *     2. JobTracker(JobPullProcessor)获取此TaskTracker对应nodeGroup的可执行任务,以及req带过来的可执行线程数, 批次(默认每批次10条)通过命令PUSH_JOB推送给TaskTracker执行
 *     3. 同时, JobTracker将此任务从executableQueue中删除,并添加进executingQueue中
 *     4. JobTrackerMStatReporter更新任务推送任务推送数量
 *     5. 如果JobTracker推送失败(比如TaskTracker上没有空闲的可执行线程等), 回滚任务从executingQueue到executableQueue
 *     6. TaskTracker(JobPushProcessor)循环批次任务, 对每条任务通过RunnerPool进行任务执行
 *     7. 执行过程为:
 *          a. 先将任务放入任务执行Map(RunningJobManager.JOBS)
 *          b. 创建任务执行类
 *          c. 进行任务执行并返回执行结果
 *          d. 将任务移除任务执行Map(RunningJobManager.JOBS)
 *          e. TaskTrackerMStatReporter进行一些必要的统计
 *          f. 设置是否接受新任务的标志(receiveNewJob)
 *          g. 执行完成后, 发送JOB_COMPLETED给JobTracker,并判断发送结果
 *          h. 如果发送失败,启动retryScheduler,先将执行结果存入failStore中(默认为LeveldbFailStore)
 *          i. retryScheduler定时任务(默认30s触发一次),从failStore获取任务执行结果(JobRunResult),发送给JobTracker
 *     8. JobTracker(JobCompletedProcessor)处理任务执行结果:
 *          a. JobStatBiz->任务执行统计(表lts_job_log_po,JobTrackerMStatReporter )
 *          b. JobProcBiz->判断此任务十分需要反馈给client, 如果需要, 通过ClientNotifier通知JobClient任务执行结果,并在接受JobClient响应后继续下面c的处理
 *          c. 判断任务是cron任务或repeat任务, 如果是的话,计算下次任务触发时间.
 *             如果时间计算不出来, 说明任务执行完成, 直接从executableQueue表中删除任务
 *             否则, 更新executableQueue表中的记录
 *             并从executingQueue表中将此任务删除
 *          d. PushNewJobBiz获取一个新的执行任务:
 *              i. 获取需要执行的任务(executableQueue表中is_running=false且triggerTime<now()),放入AbstractPreLoader.JOB_MAP对应的TaskTracker nodeGroup对应的queue中
 *              ii. 从queue中获取一个任务, 组装JobPushRequest对象, 返回给TaskTracker
 *     9. TaskTracker(JobPushProcessor.JobRunnerCallback)收到响应后,将JobPushRequest对象中的jobMeta放入response中, 并返回jobMeta
 *     10. 由于jobMeta不为空, JobRunnerDelegate中的线程循环执行, 再次出发上述步骤7的执行
 *
 * </pre>
 */
public class JobPullMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobPullMachine.class.getSimpleName());

    // 定时检查TaskTracker是否有空闲的线程，如果有，那么向JobTracker发起任务pull请求
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("LTS-JobPullMachine-Executor", true));
    private ScheduledFuture<?> scheduledFuture;
    private AtomicBoolean start = new AtomicBoolean(false);
    private TaskTrackerAppContext appContext;
    private Runnable worker;
    private int jobPullFrequency;
    // 是否启用机器资源检查
    private boolean machineResCheckEnable = false;

    public JobPullMachine(final TaskTrackerAppContext appContext) {
        this.appContext = appContext;
        this.jobPullFrequency = appContext.getConfig().getParameter(ExtConfig.JOB_PULL_FREQUENCY, Constants.DEFAULT_JOB_PULL_FREQUENCY);

        this.machineResCheckEnable = appContext.getConfig().getParameter(ExtConfig.LB_MACHINE_RES_CHECK_ENABLE, false);

        appContext.getEventCenter().subscribe(
                new EventSubscriber(JobPullMachine.class.getSimpleName().concat(appContext.getConfig().getIdentity()),
                        new Observer() {
                            @Override
                            public void onObserved(EventInfo eventInfo) {
                                if (EcTopic.JOB_TRACKER_AVAILABLE.equals(eventInfo.getTopic())) {
                                    // JobTracker 可用了
                                    start();
                                } else if (EcTopic.NO_JOB_TRACKER_AVAILABLE.equals(eventInfo.getTopic())) {
                                    stop();
                                }
                            }
                        }), EcTopic.JOB_TRACKER_AVAILABLE, EcTopic.NO_JOB_TRACKER_AVAILABLE);
        this.worker = new Runnable() {
            @Override
            public void run() {
                try {
                    if (!start.get()) {
                        return;
                    }
                    if (!isMachineResEnough()) {
                        // 如果机器资源不足,那么不去取任务
                        return;
                    }
                    sendRequest();
                } catch (Exception e) {
                    LOGGER.error("Job pull machine run error!", e);
                }
            }
        };
    }

    private void start() {
        try {
            if (start.compareAndSet(false, true)) {
                if (scheduledFuture == null) {
                    scheduledFuture = executorService.scheduleWithFixedDelay(worker, jobPullFrequency * 1000, jobPullFrequency * 1000, TimeUnit.MILLISECONDS);
                }
                LOGGER.info("Start Job pull machine success!");
            }
        } catch (Throwable t) {
            LOGGER.error("Start Job pull machine failed!", t);
        }
    }

    private void stop() {
        try {
            if (start.compareAndSet(true, false)) {
//                scheduledFuture.cancel(true);
//                executorService.shutdown();
                LOGGER.info("Stop Job pull machine success!");
            }
        } catch (Throwable t) {
            LOGGER.error("Stop Job pull machine failed!", t);
        }
    }

    /**
     * 发送Job pull 请求
     */
    private void sendRequest() throws RemotingCommandFieldCheckException {
        int availableThreads = appContext.getRunnerPool().getAvailablePoolSize();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("current availableThreads:{}", availableThreads);
        }
        if (availableThreads == 0) {
            return;
        }
        JobPullRequest requestBody = appContext.getCommandBodyWrapper().wrapper(new JobPullRequest());
        requestBody.setAvailableThreads(availableThreads);
        RemotingCommand request = RemotingCommand.createRequestCommand(JobProtos.RequestCode.JOB_PULL.code(), requestBody);

        try {
            RemotingCommand responseCommand = appContext.getRemotingClient().invokeSync(request);
            if (responseCommand == null) {
                LOGGER.warn("Job pull request failed! response command is null!");
                return;
            }
            if (JobProtos.ResponseCode.JOB_PULL_SUCCESS.code() == responseCommand.getCode()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Job pull request success!");
                }
                return;
            }
            LOGGER.warn("Job pull request failed! response command is null!");
        } catch (JobTrackerNotFoundException e) {
            LOGGER.warn("no job tracker available!");
        }
    }

    /**
     * 查看当前机器资源是否足够
     */
    private boolean isMachineResEnough() {

        if (!machineResCheckEnable) {
            // 如果没有启用,直接返回
            return true;
        }

        boolean enough = true;

        try {
            // 1. Cpu usage
            Double maxCpuTimeRate = appContext.getConfig().getParameter(ExtConfig.LB_CPU_USED_RATE_MAX, 90d);
            Object processCpuTimeRate = JVMMonitor.getAttribute(JVMConstants.JMX_JVM_THREAD_NAME, "ProcessCpuTimeRate");
            if (processCpuTimeRate != null) {
                Double cpuRate = Double.valueOf(processCpuTimeRate.toString()) / (Constants.AVAILABLE_PROCESSOR * 1.0);
                if (cpuRate >= maxCpuTimeRate) {
                    LOGGER.info("Pause Pull, CPU USAGE is " + String.format("%.2f", cpuRate) + "% >= " + String.format("%.2f", maxCpuTimeRate) + "%");
                    enough = false;
                    return false;
                }
            }

            // 2. Memory usage
            Double maxMemoryUsedRate = appContext.getConfig().getParameter(ExtConfig.LB_MEMORY_USED_RATE_MAX, 90d);
            Runtime runtime = Runtime.getRuntime();
            long maxMemory = runtime.maxMemory();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();

            Double memoryUsedRate = new BigDecimal(usedMemory / (maxMemory*1.0), new MathContext(4)).doubleValue();

            if (memoryUsedRate >= maxMemoryUsedRate) {
                LOGGER.info("Pause Pull, MEMORY USAGE is " + memoryUsedRate + " >= " + maxMemoryUsedRate);
                enough = false;
                return false;
            }
            enough = true;
            return true;
        } catch (Exception e) {
            LOGGER.warn("Check Machine Resource error", e);
            return true;
        } finally {
            Boolean machineResEnough = appContext.getConfig().getInternalData(Constants.MACHINE_RES_ENOUGH, true);
            if (machineResEnough != enough) {
                appContext.getConfig().setInternalData(Constants.MACHINE_RES_ENOUGH, enough);
            }
        }
    }
}

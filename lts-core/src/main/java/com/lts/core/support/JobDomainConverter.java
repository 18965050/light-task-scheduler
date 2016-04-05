package com.lts.core.support;

import com.lts.biz.logger.domain.JobLogPo;
import com.lts.core.commons.utils.CollectionUtils;
import com.lts.core.commons.utils.StringUtils;
import com.lts.core.constant.Constants;
import com.lts.core.domain.Job;
import com.lts.core.domain.JobMeta;
import com.lts.core.domain.JobRunResult;
import com.lts.queue.domain.JobFeedbackPo;
import com.lts.queue.domain.JobPo;

import java.util.Map;
import java.util.Set;

/**
 * @author Robert HG (254963746@qq.com) on 8/18/14.
 */
public class JobDomainConverter {

    private JobDomainConverter() {
    }

    public static JobPo convert(Job job) {
        JobPo jobPo = new JobPo();
        jobPo.setPriority(job.getPriority());
        jobPo.setTaskId(job.getTaskId());
        jobPo.setRealTaskId(jobPo.getTaskId());
        jobPo.setRealTaskId(jobPo.getTaskId());
        jobPo.setGmtCreated(SystemClock.now());
        jobPo.setGmtModified(jobPo.getGmtCreated());
        jobPo.setSubmitNodeGroup(job.getSubmitNodeGroup());
        jobPo.setTaskTrackerNodeGroup(job.getTaskTrackerNodeGroup());

        if (CollectionUtils.isNotEmpty(job.getExtParams())) {
            Set<String> removeKeySet = null;
            for (Map.Entry<String, String> entry : job.getExtParams().entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("__LTS_")) {
                    jobPo.setInternalExtParam(key, entry.getValue());
                    removeKeySet = CollectionUtils.newHashSetOnNull(removeKeySet);
                    removeKeySet.add(key);
                }
            }
            if (removeKeySet != null) {
                for (String key : removeKeySet) {
                    job.getExtParams().remove(key);
                }
            }
        }

        jobPo.setExtParams(job.getExtParams());
        jobPo.setNeedFeedback(job.isNeedFeedback());
        jobPo.setCronExpression(job.getCronExpression());
        jobPo.setMaxRetryTimes(job.getMaxRetryTimes());
        jobPo.setRelyOnPrevCycle(job.isRelyOnPrevCycle());
        jobPo.setRepeatCount(job.getRepeatCount());
        if (!jobPo.isCron()) {
            if (job.getTriggerTime() == null) {
                jobPo.setTriggerTime(SystemClock.now());
            } else {
                jobPo.setTriggerTime(job.getTriggerTime());
            }
        }
        if (job.getRepeatCount() != 0) {
            jobPo.setCronExpression(null);
            jobPo.setRepeatInterval(job.getRepeatInterval());
            jobPo.setInternalExtParam(Constants.FIRST_FIRE_TIME, String.valueOf(jobPo.getTriggerTime()));
        }
        return jobPo;
    }

    /**
     * JobPo 转 Job
     */
    public static JobMeta convert(JobPo jobPo) {
        Job job = new Job();
        job.setPriority(jobPo.getPriority());
        job.setExtParams(jobPo.getExtParams());
        job.setSubmitNodeGroup(jobPo.getSubmitNodeGroup());
        job.setTaskId(jobPo.getTaskId());
        job.setTaskTrackerNodeGroup(jobPo.getTaskTrackerNodeGroup());
        job.setNeedFeedback(jobPo.isNeedFeedback());
        job.setCronExpression(jobPo.getCronExpression());
        job.setTriggerTime(jobPo.getTriggerTime());
        job.setMaxRetryTimes(jobPo.getMaxRetryTimes() == null ? 0 : jobPo.getMaxRetryTimes());
        job.setRelyOnPrevCycle(jobPo.getRelyOnPrevCycle() == null ? false : jobPo.getRelyOnPrevCycle());
        job.setRepeatCount(jobPo.getRepeatCount());
        job.setRepeatInterval(jobPo.getRepeatInterval());
        JobMeta jobMeta = new JobMeta();
        jobMeta.setJobId(jobPo.getJobId());
        jobMeta.setJob(job);
        jobMeta.setRealTaskId(jobPo.getRealTaskId());
        jobMeta.setInternalExtParams(jobPo.getInternalExtParams());
        jobMeta.setRetryTimes(jobPo.getRetryTimes() == null ? 0 : jobPo.getRetryTimes());
        jobMeta.setRepeatedCount(jobPo.getRepeatedCount());
        return jobMeta;
    }

    public static JobLogPo convertJobLog(JobMeta jobMeta) {
        JobLogPo jobLogPo = new JobLogPo();
        jobLogPo.setGmtCreated(SystemClock.now());
        Job job = jobMeta.getJob();
        jobLogPo.setPriority(job.getPriority());
        jobLogPo.setExtParams(job.getExtParams());
        jobLogPo.setInternalExtParams(jobMeta.getInternalExtParams());
        jobLogPo.setSubmitNodeGroup(job.getSubmitNodeGroup());
        jobLogPo.setTaskId(job.getTaskId());
        jobLogPo.setRealTaskId(jobMeta.getRealTaskId());
        jobLogPo.setTaskTrackerNodeGroup(job.getTaskTrackerNodeGroup());
        jobLogPo.setNeedFeedback(job.isNeedFeedback());
        jobLogPo.setRetryTimes(jobMeta.getRetryTimes());
        jobLogPo.setMaxRetryTimes(job.getMaxRetryTimes());
        jobLogPo.setDepPreCycle(jobMeta.getJob().isRelyOnPrevCycle());
        jobLogPo.setJobId(jobMeta.getJobId());
        jobLogPo.setCronExpression(job.getCronExpression());
        jobLogPo.setTriggerTime(job.getTriggerTime());

        jobLogPo.setRepeatCount(job.getRepeatCount());
        jobLogPo.setRepeatedCount(jobMeta.getRepeatedCount());
        jobLogPo.setRepeatInterval(job.getRepeatInterval());
        return jobLogPo;
    }

    public static JobLogPo convertJobLog(JobPo jobPo) {
        JobLogPo jobLogPo = new JobLogPo();
        jobLogPo.setGmtCreated(SystemClock.now());
        jobLogPo.setPriority(jobPo.getPriority());
        jobLogPo.setExtParams(jobPo.getExtParams());
        jobLogPo.setInternalExtParams(jobPo.getInternalExtParams());
        jobLogPo.setSubmitNodeGroup(jobPo.getSubmitNodeGroup());
        jobLogPo.setTaskId(jobPo.getTaskId());
        jobLogPo.setRealTaskId(jobPo.getRealTaskId());
        jobLogPo.setTaskTrackerNodeGroup(jobPo.getTaskTrackerNodeGroup());
        jobLogPo.setNeedFeedback(jobPo.isNeedFeedback());
        jobLogPo.setJobId(jobPo.getJobId());
        jobLogPo.setCronExpression(jobPo.getCronExpression());
        jobLogPo.setTriggerTime(jobPo.getTriggerTime());
        jobLogPo.setTaskTrackerIdentity(jobPo.getTaskTrackerIdentity());
        jobLogPo.setRetryTimes(jobPo.getRetryTimes());
        jobLogPo.setMaxRetryTimes(jobPo.getMaxRetryTimes());
        jobLogPo.setDepPreCycle(jobPo.getRelyOnPrevCycle());

        jobLogPo.setRepeatCount(jobPo.getRepeatCount());
        jobLogPo.setRepeatedCount(jobPo.getRepeatedCount());
        jobLogPo.setRepeatInterval(jobPo.getRepeatInterval());
        return jobLogPo;
    }

    public static JobFeedbackPo convert(JobRunResult result) {
        JobFeedbackPo jobFeedbackPo = new JobFeedbackPo();
        jobFeedbackPo.setJobRunResult(result);
        jobFeedbackPo.setId(StringUtils.generateUUID());
        jobFeedbackPo.setGmtCreated(SystemClock.now());
        return jobFeedbackPo;
    }

}

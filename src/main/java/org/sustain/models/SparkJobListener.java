package org.sustain.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.scheduler.*;

public class SparkJobListener extends SparkListener {

    private static final Logger log = LogManager.getLogger(SparkJobListener.class);

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        log.info("StageCompleted invoked");
        super.onStageCompleted(stageCompleted);
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        log.info("StageSubmitted invoked");
        super.onStageSubmitted(stageSubmitted);
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        log.info("TaskStart invoked");
        super.onTaskStart(taskStart);
    }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        log.info("TaskGettingResult invoked");
        super.onTaskGettingResult(taskGettingResult);
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        log.info("TaskEnd invoked");
        super.onTaskEnd(taskEnd);
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        log.info("JobStart invoked");
        super.onJobStart(jobStart);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        log.info("JobEnd invoked");
        super.onJobEnd(jobEnd);
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
        super.onEnvironmentUpdate(environmentUpdate);
    }

    @Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
        super.onBlockManagerAdded(blockManagerAdded);
    }

    @Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
        super.onBlockManagerRemoved(blockManagerRemoved);
    }

    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
        super.onUnpersistRDD(unpersistRDD);
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log.info("ApplicationStart invoked");
        super.onApplicationStart(applicationStart);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        log.info("ApplicationEnd invoked");
        super.onApplicationEnd(applicationEnd);
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        super.onExecutorMetricsUpdate(executorMetricsUpdate);
    }

    @Override
    public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
        super.onStageExecutorMetrics(executorMetrics);
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        log.info("ExecutorAdded invoked");
        super.onExecutorAdded(executorAdded);
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        log.info("ExecutorRemoved invoked");
        super.onExecutorRemoved(executorRemoved);
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        super.onExecutorBlacklisted(executorBlacklisted);
    }

    @Override
    public void onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
        super.onExecutorBlacklistedForStage(executorBlacklistedForStage);
    }

    @Override
    public void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
        super.onNodeBlacklistedForStage(nodeBlacklistedForStage);
    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        super.onExecutorUnblacklisted(executorUnblacklisted);
    }

    @Override
    public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
        super.onNodeBlacklisted(nodeBlacklisted);
    }

    @Override
    public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
        super.onNodeUnblacklisted(nodeUnblacklisted);
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        super.onBlockUpdated(blockUpdated);
    }

    @Override
    public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
        super.onSpeculativeTaskSubmitted(speculativeTask);
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        super.onOtherEvent(event);
    }
}

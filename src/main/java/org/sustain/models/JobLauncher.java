package org.sustain.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.scheduler.*;


public class JobLauncher implements SparkAppHandle.Listener {

    private static final Logger log = LogManager.getLogger(JobLauncher.class);


    public SparkAppHandle launchJob(String mainClass, String[] args) throws Exception {

        String appResource = "build/libs/shadow.jar";
        String sparkMaster = "spark://lattice-167:8079";
        String deployMode = "client";

        SparkAppHandle handle = new SparkLauncher()
                .setAppResource(appResource).addAppArgs(args)
                .setMainClass(mainClass)
                .setMaster(sparkMaster)
                .redirectToLog(getClass().getName())
                .startApplication(this);

        log.info("Launched [" + mainClass + "] from [" + appResource + "] State [" + handle.getState() + "]");

        return handle;
    }

    /**
     * Callback method for changes to the Spark Job
     */
    @Override
    public void infoChanged(SparkAppHandle handle) {

        log.info("Spark App Id [" + handle.getAppId() + "] Info Changed.  State [" + handle.getState() + "]");

    }

    /**
     * Callback method for changes to the Spark Job's state
     */
    @Override
    public void stateChanged(SparkAppHandle handle) {

        log.info("Spark App Id [" + handle.getAppId() + "] State Changed. State [" + handle.getState() + "]");

    }
}
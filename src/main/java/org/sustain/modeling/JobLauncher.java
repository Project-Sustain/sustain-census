package org.sustain.modeling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;


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
                .setDeployMode(deployMode)
                .redirectOutput(new File("spark-output"))
                .redirectError(new File("spark-error"))
                .startApplication(this);

        System.setProperty("java.util.logging.SimpleFormatter.format","%5$s%6$s%n");

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

    private java.util.logging.Logger createLogger(String appName) throws IOException {
        final java.util.logging.Logger logger = getRootLogger();
        final FileHandler handler = new FileHandler("./" + appName + "-%u-%g.log", 10_000_000, 5, true);
        handler.setFormatter(new SimpleFormatter());
        logger.addHandler(handler);
        logger.setLevel(Level.INFO);
        return logger;
    }

    private java.util.logging.Logger getRootLogger() {
        final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        Arrays.stream(logger.getHandlers()).forEach(logger::removeHandler);
        //Without this the logging will go to the Console and to a file.
        logger.setUseParentHandlers(false);
        return logger;
    }
}
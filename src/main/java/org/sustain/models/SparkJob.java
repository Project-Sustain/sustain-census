package org.sustain.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.scheduler.SparkListener;

import java.io.File;

public class SparkJob {

    private static final Logger log = LogManager.getLogger(SparkJob.class);

    public static void main(String[] args) {
        System.out.println("Running an Apache Spark model...");

        // Create SparkLauncher for programmatically submitting a Spark job
        JobLauncher launcher = new JobLauncher();

        // Launch the app
        try {
            launcher.launchJob("org.sustain.HelloWorld", new String[]{});

            while (true) {

            }

        } catch (Exception e) {
            System.out.println("Caught Exception: " + e.toString());
            System.out.println("Stacktrace: " + e.getCause().toString());
        }

    }
}

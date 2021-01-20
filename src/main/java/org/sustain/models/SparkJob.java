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
        SparkLauncher launcher = new SparkLauncher();

        launcher.setMaster("spark://lattice-167:8079")
                .setAppResource("build/libs/shadow.jar") // Specify user app jar path
                .setMainClass("org.sustain.HelloWorld")
                .setVerbose(true)
                .setDeployMode("client")
                .redirectOutput(new File("spark-output.txt"))
                .redirectError(new File("spark-err.txt"));


        // Launch the app
        try {
            String[] appArgs = {};
            JobLauncher jobLauncher = new JobLauncher();
            SparkAppHandle appHandle = jobLauncher.launchJob("org.sustain.HelloWorld", appArgs);

            appHandle.wait();
        } catch (Exception e) {
            System.out.println("Caught Exception: " + e.toString());
        }

    }
}

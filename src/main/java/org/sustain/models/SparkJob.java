package org.sustain.models;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class SparkJob {

    public static void main(String[] args) {
        System.out.println("Running an Apache Spark model...");

        // Create SparkLauncher for programmatically submitting a Spark job
        SparkLauncher launcher = new SparkLauncher();
        launcher.setMaster("spark://lattice-167:8079")
                .setAppResource("build/libs/shadow.jar") // Specify user app jar path
                .setMainClass("org.sustain.HelloWorld");

        // Launch the app
        try {
            launcher.launch();
        } catch (IOException e) {
            System.out.println("Caught IOException.");
        }

    }
}

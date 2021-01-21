package org.sustain.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.scheduler.SparkListener;
import java.time.Duration;

import java.io.File;
import java.util.Scanner;

public class SparkJob {

    private static final Logger log = LogManager.getLogger(SparkJob.class);

    public static void main(String[] args) {
        System.out.println("Running an Apache Spark model...");

        // Create SparkLauncher for programmatically submitting a Spark job
        JobLauncher launcher = new JobLauncher();

        // Launch the app
        try {
            String[] appArgs = {
                    "modelType=LinearRegression",
                    "databaseName=sustaindb",
                    "databaseHost=mongo://lattice-46:27017",
                    "collection=future_heat",
                    "sparkMaster=spark://lattice-167:8079"
            };

            SparkAppHandle appHandle = launcher.launchJob("org.sustain.SparkModel", appArgs);

            // Spin until app state is finished
            while (!appHandle.getState().isFinal()) {;}

            // Read the results from the spark-output file
            Scanner resultScanner = new Scanner(new File("spark-output"));
            while (resultScanner.hasNextLine()) {
                String line = resultScanner.nextLine();
                log.info(line);
            }
            resultScanner.close();

        } catch (Exception e) {
            System.out.println("Caught Exception: " + e.toString());
            System.out.println("Stacktrace: " + e.getCause().toString());
        }

    }
}

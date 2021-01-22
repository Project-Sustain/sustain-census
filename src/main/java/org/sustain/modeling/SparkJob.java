package org.sustain.modeling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import java.io.File;
import java.util.Scanner;

public class SparkJob {

    private static final Logger log = LogManager.getLogger(SparkJob.class);

    public static void main(String[] args) {
        System.out.println("Running an Apache Spark model...");

        String exampleRequest = "{\n" +
                "{\n" +
                "   \"collection\": \"future_heat\",\n" +
                "   \"feature\": \"year\",\n" +
                "   \"label\": \"temp\"\n" +
                "   \"GISJOINS\": [" +
                "       \"G1201050\",\n" +
                "       \"G4804550\",\n" +
                "       \"G4500890\"\n" +
                "   ]\n" +
                "}";

        SparkJob sparkJob = new SparkJob();
        String jsonResponse = sparkJob.createSparkJob(exampleRequest);
        System.out.println(jsonResponse);

        /*
        // Create SparkLauncher for programmatically submitting a Spark job
        JobLauncher launcher = new JobLauncher();

        // Launch the app
        try {
            String[] appArgs = {
                    "modelType=LinearRegression",
                    "databaseName=sustaindb",
                    "databaseHost=mongodb://lattice-46:27017",
                    "collection=future_heat",
                    "sparkMaster=spark://lattice-167:8079",
                    "gisJoins=G1201050,G4804550,G4500890"
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
        */


    }

    // Returns JSON String
    public String createSparkJob(String jsonRequest) {
        return jsonRequest;
    }
}

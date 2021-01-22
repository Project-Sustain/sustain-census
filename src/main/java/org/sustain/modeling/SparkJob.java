package org.sustain.modeling;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class SparkJob {

    private static final Logger log = LogManager.getLogger(SparkJob.class);

    public static void main(String[] args) {
        System.out.println("Running an Apache Spark model...");

        String exampleRequest = "{\n" +
                "   \"collection\": \"future_heat\",\n" +
                "   \"feature\": \"year\",\n" +
                "   \"label\": \"temp\",\n" +
                "   \"gisJoins\": [\n" +
                "       \"G1201050\",\n" +
                "       \"G4804550\",\n" +
                "       \"G4500890\"\n" +
                "   ]\n" +
                "}";


        SparkJob sparkJob = new SparkJob();
        String jsonResponse = sparkJob.createSparkJob(exampleRequest);
        log.info(jsonResponse);

    }

    // Returns JSON String
    public String createSparkJob(String jsonRequest) {
        Gson gson = new Gson();

        log.info(jsonRequest);
        LinearRegressionParams lr = gson.fromJson(jsonRequest, LinearRegressionParams.class);

        ArrayList<String> appArgs = new ArrayList<String>();
        appArgs.add("modelType=LinearRegressionParams");
        appArgs.add("databaseName=sustaindb");
        appArgs.add("databaseHost=mongodb://lattice-46:27017");
        appArgs.add("collection=" + lr.collection);
        appArgs.add("feature=" + lr.feature);
        appArgs.add("label=" + lr.label);

        String gisJoins = "gisJoins=";
        for (int i = 0; i < lr.gisJoins.size(); i++) {
            String gisJoin = lr.gisJoins.get(i);
            gisJoins += gisJoin;
            if (i < lr.gisJoins.size()-1) {
                gisJoins += ",";
            }
        }
        appArgs.add(gisJoins);

        String[] appArgsArray = (String[]) appArgs.toArray();
        JobLauncher launcher = new JobLauncher();

        String jsonResults = "";
        try {
            SparkAppHandle appHandle = launcher.launchJob("org.sustain.SparkModel", appArgsArray);

            // Spin until app state is finished
            while (!appHandle.getState().isFinal()) {;}

            jsonResults = processSparkJobResults("spark-output");

        } catch (Exception e) {
            System.out.println("Caught Exception: " + e.toString());
            System.out.println("Stacktrace: " + e.getCause().toString());
        }

        return jsonResults;
    }

    private String processSparkJobResults(String outputFilename) throws IOException {

        LinearRegressionResults lrResults = new LinearRegressionResults();

        // Hardcoded for now
        lrResults.collection = "future_heat";
        lrResults.feature = "year";
        lrResults.label = "temp";
        lrResults.results = new ArrayList<LinearRegressionResult>();

        // Read the results from the output file
        Scanner resultScanner = new Scanner(new File(outputFilename));
        while (resultScanner.hasNextLine()) {
            String line = resultScanner.nextLine();
            log.info(line);

            if (line.contains(">>> Results: {")) {
                String[] resultCsv = line.replaceAll("}", "").split("\\{"); // splits into 2 parts
                resultCsv = resultCsv[1].trim().split(",");

                LinearRegressionResult lrResult = new LinearRegressionResult();
                lrResult.gisJoin = resultCsv[0];
                lrResult.coefficient = Double.parseDouble(resultCsv[1]);
                lrResult.intercept = Double.parseDouble(resultCsv[2]);
                lrResult.rmse = Double.parseDouble(resultCsv[3]);
                lrResult.predictedMax2021 = Double.parseDouble(resultCsv[4]);

                lrResults.results.add(lrResult);
            }
        }
        resultScanner.close();

        Gson gson = new Gson();
        return gson.toJson(lrResults);
    }
}

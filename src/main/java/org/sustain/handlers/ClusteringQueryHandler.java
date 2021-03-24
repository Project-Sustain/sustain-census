package org.sustain.handlers;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.sustain.BisectingKMeansResponse;
import org.sustain.GaussianMixtureResponse;
import org.sustain.KMeansClusteringResponse;
import org.sustain.LatentDirichletAllocationResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ClusteringQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {

    private static final Logger log = LogManager.getFormatterLogger(ClusteringQueryHandler.class);

    public ClusteringQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public boolean isValid(ModelRequest modelRequest) {
        // TODO: Implement
        return true;
    }

    @Override
    public void handleRequest() {
        this.logRequest(request);

        try {
            // Submit task to Spark Manager
            Future<Boolean> future =
				this.sparkManager.submit(this, "clustering-query");

            // Wait for task to complete
            future.get();
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public Boolean execute(JavaSparkContext sparkContext, SQLContext sqlContext) {
        switch (request.getType()) {
            case K_MEANS_CLUSTERING:
                buildKMeansModel(sparkContext);
                break;
            case BISECTING_K_MEANS:
                buildBisectingKMeansModel(sparkContext);
                break;
            case GAUSSIAN_MIXTURE:
                buildGaussianMixtureModel(sparkContext);
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                buildLatentDirichletAllocationModel(sparkContext);
                break;
        }

        return true;
    }

    private void buildLatentDirichletAllocationModel(JavaSparkContext sparkContext) {
        int k = request.getLatentDirichletAllocationRequest().getClusterCount();
        int maxIterations = request.getLatentDirichletAllocationRequest().getMaxIterations();
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);

        // LDA
        LDA lda = new LDA().setK(k).setMaxIter(maxIterations);
        LDAModel model = lda.fit(featureDF);

        double ll = model.logLikelihood(featureDF);
        double lp = model.logPerplexity(featureDF);
        log.info("LDA: Lower bound on the log likelihood of the entire corpus: " + ll);
        log.info("LDA: Upper bound on perplexity: " + lp);

        // results
        Dataset<Row> predictDF = model.transform(featureDF);
        log.info("Predictions...");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing LatentDirichletAllocationResponse to stream");
        for (ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                    .setLatentDirichletAllocationResponse(
                            LatentDirichletAllocationResponse.newBuilder()
                                    .setGisJoin(result.getGisJoin())
                                    .setPrediction(result.getPrediction())
                                    .build()
                    ).build()
            );
        }
    }

    private void buildKMeansModel(JavaSparkContext sparkContext) {
        int k = request.getKMeansClusteringRequest().getClusterCount();
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);
        // KMeans Clustering
        KMeans kmeans = new KMeans().setK(k).setSeed(1L);
        KMeansModel model = kmeans.fit(featureDF);

        Vector[] vectors = model.clusterCenters();
        log.info("======================== CLUSTER CENTERS =====================================");
        for (Vector vector : vectors) {
            log.info(vector.toString());
        }

        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing KMeansClusteringResponses to stream");
        for (ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                    .setKMeansClusteringResponse(
                            KMeansClusteringResponse.newBuilder()
                                    .setGisJoin(result.getGisJoin())
                                    .setPrediction(result.getPrediction())
                                    .build()
                    ).build()
            );
        }
    }

    private void buildBisectingKMeansModel(JavaSparkContext sparkContext) {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);
        int k = request.getBisectingKMeansRequest().getClusterCount();
        int maxIterations = request.getBisectingKMeansRequest().getMaxIterations();

        BisectingKMeans bisectingKMeans = new BisectingKMeans().setK(k).setMaxIter(maxIterations);
        BisectingKMeansModel model = bisectingKMeans.fit(featureDF);

        // Make predictions
        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions ...");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing BisectingKMeansResponses to stream");
        for (ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                    .setBisectingKMeansResponse(
                            BisectingKMeansResponse.newBuilder()
                                    .setGisJoin(result.getGisJoin())
                                    .setPrediction(result.getPrediction())
                                    .build()
                    ).build()
            );
        }
    }

    private void buildGaussianMixtureModel(JavaSparkContext sparkContext) {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);
        int k = request.getGaussianMixtureRequest().getClusterCount();
        int maxIterations = request.getGaussianMixtureRequest().getMaxIterations();

        GaussianMixture gaussianMixture = new GaussianMixture().setK(k).setMaxIter(maxIterations);
        GaussianMixtureModel model = gaussianMixture.fit(featureDF);


        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
                    i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }

        // Make predictions
        Dataset<Row> predictDF = model.transform(featureDF).select(Constants.GIS_JOIN, "prediction");
        log.info("Predictions ...");
        predictDF.show(10);

        Dataset<String> jsonResults = predictDF.toJSON();
        String jsonString = jsonResults.collectAsList().toString();

        Gson gson = new Gson();
        Type type = new TypeToken<List<ClusteringResult>>() {
        }.getType();
        List<ClusteringResult> results = gson.fromJson(jsonString, type);
        log.info("results.size(): " + results.size());

        log.info("Writing GaussianMixtureResponses to stream");
        for (ClusteringResult result : results) {
            responseObserver.onNext(ModelResponse.newBuilder()
                    .setGaussianMixtureResponse(
                            GaussianMixtureResponse.newBuilder()
                                    .setGisJoin(result.getGisJoin())
                                    .setPrediction(result.getPrediction())
                                    .build()
                    ).build()
            );
        }
    }

    private Dataset<Row> preprocessAndGetFeatureDF(JavaSparkContext sparkContext) {
		// Identify resolution of evaluation
        String resolution = "";
        switch (request.getType()) {
            case K_MEANS_CLUSTERING:
                resolution = request.getKMeansClusteringRequest().getResolution().toString().toLowerCase();
                break;
            case BISECTING_K_MEANS:
                resolution = request.getBisectingKMeansRequest().getResolution().toString().toLowerCase();
                break;
            case GAUSSIAN_MIXTURE:
                resolution = request.getGaussianMixtureRequest().getResolution().toString().toLowerCase();
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                resolution = request.getLatentDirichletAllocationRequest().getResolution().toString().toLowerCase();
                break;
            case UNRECOGNIZED:
                log.warn("Invalid request type");
                break;
        }

		// Initialize mongodb read configuration
        Map<String, String> readOverrides = new HashMap();
        readOverrides.put("spark.mongodb.input.collection", resolution + "_stats");
        readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
        readOverrides.put("spark.mongodb.input.uri",
            "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

        ReadConfig readConfig = 
            ReadConfig.create(sparkContext.getConf(), readOverrides);

		// Load mongodb rdd and convert to dataset
        log.info("Preprocessing data");
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();
        List<String> featuresList = new ArrayList<>(request.getCollections(0).getFeaturesList());
        Seq<String> features = convertListToSeq(featuresList);

        Dataset<Row> selectedFeatures = collection.select(Constants.GIS_JOIN, features);

        // Dropping rows with null values
        selectedFeatures = selectedFeatures.na().drop();

        // Assembling
        VectorAssembler assembler =
                new VectorAssembler().setInputCols(featuresList.toArray(new String[0])).setOutputCol("features");
        Dataset<Row> featureDF = assembler.transform(selectedFeatures);
        featureDF.show(10);

        // Scaling
        log.info("Normalizing features");
        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("normalized_features");
        MinMaxScalerModel scalerModel = scaler.fit(featureDF);

        featureDF = scalerModel.transform(featureDF);
        featureDF = featureDF.drop("features");
        featureDF = featureDF.withColumnRenamed("normalized_features", "features");

        log.info("Dataframe after min-max normalization");
        featureDF.show(10);

        return featureDF;
    }


    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    private static class ClusteringResult {
        @SerializedName("GISJOIN")
        String gisJoin;
        int prediction;

        public String getGisJoin() {
            return gisJoin;
        }

        public int getPrediction() {
            return prediction;
        }

        @Override
        public String toString() {
            return "ClusteringResult{" +
                    "gisJoin='" + gisJoin + '\'' +
                    ", prediction=" + prediction +
                    '}';
        }
    }
}

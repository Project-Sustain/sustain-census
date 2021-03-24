package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.sustain.Collection;
import org.sustain.LinearRegressionRequest;
import org.sustain.LinearRegressionResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.modeling.LinearRegressionModelImpl;
import org.sustain.util.Constants;
import org.sustain.util.Profiler;
import org.apache.spark.util.SizeEstimator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class RegressionQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public void handleRequest() {
        if (isValid(this.request)) {
            logRequest(this.request);
			try {
				// Submit task to Spark Manager
				Future<Boolean> future =
					this.sparkManager.submit(this, "regression-query");

				// Wait for task to complete
				future.get();

				responseObserver.onCompleted();
			} catch (Exception e) {
				log.error("Failed to evaluate query", e);
				responseObserver.onError(e);
			}
        } else {
            log.warn("Invalid Model Request!");
        }
    }

    @Override
    public Boolean execute(JavaSparkContext sparkContext, SQLContext sqlContext) {
		Profiler profiler = new Profiler();
		profiler.addTask("LINEAR_REGRESSION_MODELS");
		profiler.indent();

		// Set parameters of Linear Regression Model
		LinearRegressionRequest lrRequest = this.request.getLinearRegressionRequest();
		Collection requestCollection = this.request.getCollections(0); // We only support 1 collection currently

		String mongoUri = String.format("mongodb://%s:%s", Constants.DB.HOST, Constants.DB.PORT);

		// Create a custom ReadConfig
		profiler.addTask("CREATE_READ_CONFIG");
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("uri", mongoUri);
		readOverrides.put("database", Constants.DB.NAME);
		readOverrides.put("collection", requestCollection.getName());
		ReadConfig readConfig = ReadConfig.create(sparkContext.getConf(), readOverrides);
		profiler.completeTask("CREATE_READ_CONFIG");

		// Lazy-load the collection in as a DF
		profiler.addTask("LOAD_MONGO_COLLECTION");
		Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, readConfig).toDF();
		Dataset<Row> checkPointed = mongoCollection.localCheckpoint(true);
		log.info(">>> mongoCollection Size: {}", SizeEstimator.estimate(checkPointed));
		profiler.completeTask("LOAD_MONGO_COLLECTION");

		// Build and run a model for each GISJoin in the request

		for (String gisJoin: lrRequest.getGisJoinsList()) {

			String modelTaskName = String.format("MODEL_GISJOIN_%s", gisJoin);

			profiler.addTask(modelTaskName);
			profiler.indent();

			LinearRegressionModelImpl model = new LinearRegressionModelImpl.LinearRegressionModelBuilder()
					.forMongoCollection(checkPointed)
					.forGISJoin(gisJoin)
					.forFeatures(requestCollection.getFeaturesList())
					.forLabel(requestCollection.getLabel())
					.withLoss(lrRequest.getLoss())
					.withSolver(lrRequest.getSolver())
					.withAggregationDepth(lrRequest.getAggregationDepth())
					.withMaxIterations(lrRequest.getMaxIterations())
					.withElasticNetParam(lrRequest.getElasticNetParam())
					.withEpsilon(lrRequest.getEpsilon())
					.withRegularizationParam(lrRequest.getRegularizationParam())
					.withTolerance(lrRequest.getConvergenceTolerance())
					.withFitIntercept(lrRequest.getFitIntercept())
					.withStandardization(lrRequest.getSetStandardization())
					.build();

			model.buildAndRunModel(profiler); // Launches the Spark Model

			LinearRegressionResponse modelResults = LinearRegressionResponse.newBuilder()
					.setGisJoin(model.getGisJoin())
					.setTotalIterations(model.getTotalIterations())
					.setRmseResidual(model.getRmse())
					.setR2Residual(model.getR2())
					.setIntercept(model.getIntercept())
					.addAllSlopeCoefficients(model.getCoefficients())
					.addAllObjectiveHistory(model.getObjectiveHistory())
					.build();

			ModelResponse response = ModelResponse.newBuilder()
					.setLinearRegressionResponse(modelResults)
					.build();

			logResponse(response);
			profiler.completeTask(modelTaskName);
			profiler.unindent();
			this.responseObserver.onNext(response);
		}
		profiler.completeTask("LINEAR_REGRESSION_MODELS");
		profiler.unindent();
		log.info(profiler.toString());

		return true;
    }


    @Override
    public boolean isValid(ModelRequest modelRequest) {
        if (modelRequest.getType().equals(ModelType.LINEAR_REGRESSION)) {
            if (modelRequest.getCollectionsCount() == 1) {
                if (modelRequest.getCollections(0).getFeaturesCount() == 1) {
                    return modelRequest.hasLinearRegressionRequest();
                }
            }
        }
        return false;
    }
}

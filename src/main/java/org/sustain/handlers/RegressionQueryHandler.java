package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;


public class RegressionQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {

    private static final Logger log = LogManager.getLogger(RegressionQueryHandler.class);

    public RegressionQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

	/**
	 * Compiles a List<String> of column names we desire from the loaded collection, using the features String array.
	 * @return A Scala Seq<String> of desired column names.
	 */
	private Seq<String> desiredColumns(List<String> features, String label) {
		List<String> cols = new ArrayList<>();
		cols.add("gis_join");
		cols.addAll(features);
		cols.add(label);
		return convertListToSeq(cols);
	}

	/**
	 * Converts a Java List<String> of inputs to a Scala Seq<String>
	 * @param inputList The Java List<String> we wish to transform
	 * @return A Scala Seq<String> representing the original input list
	 */
	public Seq<String> convertListToSeq(List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	/**
	 * Returns the human-readable amount of bytes, using prefixes.
	 * @param bytes Long count of bytes
	 * @return # bytes, KB, MB, GB, depending on number of input bytes.
	 */
	private String readableBytes(Long bytes) {
		if (bytes < 1024) {
			return String.format("%d bytes", bytes);
		} else if (bytes < Math.pow(1024, 2)) {
			return String.format("%.2f KB", bytes / 1024.0);
		} else if (bytes < Math.pow(1024, 3)) {
			return String.format("%.2f MB", bytes / Math.pow(1024, 2));
		} else {
			return String.format("%.2f GB", bytes / Math.pow(1024, 3));
		}
	}

	/**
	 * Transforms and processes a lazily-loaded mongodb collection (Dataset<Row>) by reference, to be used
	 * for each linear regression model per GISJoin.
	 * @param lrRequest The gRPC Linear Regression request object.
	 * @param requestCollection The gRPC Collection request object.
	 * @param mongoCollection The lazily-loaded mongodb collection.
	 */
	private Dataset<Row> processCollection(LinearRegressionRequest lrRequest, Collection requestCollection,
										   Dataset<Row> mongoCollection) {

		/*
			SQL Select only _id, gis_join, features, and label columns, and discard the rest.
			Then, rename the label column to "label", and features column to "features":
			+--------------------+--------+---------+-----------------------+
			|                 _id|gis_join|timestamp|max_max_air_temperature|
			+--------------------+--------+---------+-----------------------+
			|[6024fe7dc1d226e5...|G0100130|788918400|                291.954|
			|[6024fe7dc1d226e5...|G0100190|788918400|                288.388|
			|[6024fe7dc1d226e5...|G0100230|788918400|                290.876|
			|[6024fe7dc1d226e5...|G0100210|788918400|                290.245|
			|[6024fe7dc1d226e5...|G0100290|788918400|                289.801|
			+--------------------+--------+---------+-----------------------+
		 */
		mongoCollection = mongoCollection.select("_id",
				desiredColumns(requestCollection.getFeaturesList(), requestCollection.getLabel())
		);
		mongoCollection.show(5);

		/*
			Rename all the features columns to "feature_0, feature_1, ..., feature_n", and label column to "label".
			This gives our Dataset<Row> generalized column names, which allows for LR model flexibility:
			+--------------------+--------+---------+-------+
			|                 _id|gis_join|feature_0|  label|
			+--------------------+--------+---------+-------+
			|[6024fe7dc1d226e5...|G0100130|788918400|291.954|
			|[6024fe7dc1d226e5...|G0100190|788918400|288.388|
			|[6024fe7dc1d226e5...|G0100230|788918400|290.876|
			|[6024fe7dc1d226e5...|G0100210|788918400|290.245|
			|[6024fe7dc1d226e5...|G0100290|788918400|289.801|
			+--------------------+--------+---------+-------+
		 */
		int featuresIndex = 0;
		List<String> featureColumns = new ArrayList<>();
		mongoCollection = mongoCollection.withColumnRenamed(requestCollection.getLabel(), "label");
		for (String feature: requestCollection.getFeaturesList()) {
			String featureColumnName = String.format("feature_%d", featuresIndex);
			mongoCollection = mongoCollection.withColumnRenamed(feature, featureColumnName);
			featureColumns.add(featureColumnName);
			featuresIndex++;
		}
		mongoCollection.show(5);

		/*
			SQL Filter by the GISJoins that they requested (i.e. WHERE gis_join IN ( value1, value2, value3 ) )
			This greatly reduces the size of the Dataset:
			+--------------------+--------+---------+-------+
			|                 _id|gis_join|feature_0|  label|
			+--------------------+--------+---------+-------+
			|[6024fe7dc1d226e5...|G0100190|788918400|288.388|
			|[6024fe7ec1d226e5...|G0100190|789004800|281.047|
			|[6024fe7fc1d226e5...|G0100190|789091200|282.115|
			|[6024fe80c1d226e5...|G0100190|789177600|286.618|
			|[6024fe80c1d226e5...|G0100190|789264000|288.166|
			+--------------------+--------+---------+-------+

		 */
		mongoCollection = mongoCollection.filter(mongoCollection.col("gis_join")
				.isInCollection(lrRequest.getGisJoinsList()));
		mongoCollection.show(5);

		/*
			Assemble all the feature columns into a row-oriented Vector:
			+--------------------+--------+---------+-------+------------+
			|                 _id|gis_join|feature_0|  label|    features|
			+--------------------+--------+---------+-------+------------+
			|[6024fe7dc1d226e5...|G0100190|788918400|288.388|[7.889184E8]|
			|[6024fe7ec1d226e5...|G0100190|789004800|281.047|[7.890048E8]|
			|[6024fe7fc1d226e5...|G0100190|789091200|282.115|[7.890912E8]|
			|[6024fe80c1d226e5...|G0100190|789177600|286.618|[7.891776E8]|
			|[6024fe80c1d226e5...|G0100190|789264000|288.166| [7.89264E8]|
			+--------------------+--------+---------+-------+------------+
		 */
		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(featureColumns.toArray(new String[0]))
				.setOutputCol("features");
		return vectorAssembler.transform(mongoCollection);
	}

	/**
	 * Creates and returns a custom ReadConfig to override the SparkContext's read configuration.
	 * This is used to point the SparkContext at a specific mongo collection to read.
	 * @param sparkContext The JavaSparkContext instance for the application.
	 * @param collectionName The name of the mongo collection we want to read.
	 * @return The ReadConfig object for the SparkContext.
	 */
	private ReadConfig createReadConfig(JavaSparkContext sparkContext, String collectionName) {
		String mongoUri = String.format("mongodb://%s:%s", Constants.DB.HOST, Constants.DB.PORT);
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("uri", mongoUri);
		readOverrides.put("database", Constants.DB.NAME);
		readOverrides.put("collection", collectionName);
		return ReadConfig.create(sparkContext.getConf(), readOverrides);
	}

	private void addJars(JavaSparkContext sparkContext) {
		String[] sparkJarPaths = {
				"build/libs/scala-collection-compat_2.12-2.1.1.jar",
				"build/libs/scala-library-2.12.11.jar",
				"build/libs/scala-xml_2.12-1.2.0.jar",
				"build/libs/scala-parser-combinators_2.12-1.1.2.jar",
				"build/libs/mongo-spark-connector_2.12-3.0.1.jar",
				"build/libs/spark-core_2.12-3.0.1.jar",
				"build/libs/spark-mllib_2.12-3.0.1.jar",
				"build/libs/spark-sql_2.12-3.0.1.jar",
				"build/libs/bson-4.0.5.jar",
				"build/libs/mongo-java-driver-3.12.8.jar"
		};

		for (String jar: sparkJarPaths) {
			sparkContext.addJar(jar);
		}
	}

    @Override
    public void handleRequest() {
        if (isValid(this.request)) {
            logRequest(this.request);
			try {
				// Submit task to Spark Manager
				Future<Boolean> future = this.sparkManager.submit(this, "regression-query");

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
    public Boolean execute(JavaSparkContext sparkContext) {
		//addJars(sparkContext);
		Profiler profiler = new Profiler();
		profiler.addTask("LINEAR_REGRESSION_MODELS");
		profiler.indent();

		// Set parameters of Linear Regression Model
		LinearRegressionRequest lrRequest = this.request.getLinearRegressionRequest();
		Collection requestCollection = this.request.getCollections(0); // We only support 1 collection currently

		// Lazy-load the collection in as a DF (Dataset<Row>), transform/process it, then persist it to be reused
		// by each linear regression model per GISJoin
		ReadConfig mongoReadConfig = createReadConfig(sparkContext, requestCollection.getName());
		Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, mongoReadConfig).toDF();
		mongoCollection = processCollection(lrRequest, requestCollection, mongoCollection);
		mongoCollection.persist();

		// Build and run a model for each GISJoin in the request
		for (String gisJoin: lrRequest.getGisJoinsList()) {

			String modelTaskName = String.format("MODEL_GISJOIN_%s", gisJoin);
			profiler.addTask(modelTaskName);
			profiler.indent();

			LinearRegressionModelImpl model = new LinearRegressionModelImpl.LinearRegressionModelBuilder()
					.forMongoCollection(mongoCollection)
					.forGISJoin(gisJoin)
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

		// Unpersist collection and complete task
		mongoCollection.unpersist(true);
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

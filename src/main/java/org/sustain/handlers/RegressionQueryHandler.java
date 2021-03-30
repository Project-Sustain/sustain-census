package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
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
import scala.Function2;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.spark.api.java.StorageLevels.MEMORY_ONLY;

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
    public Boolean execute(JavaSparkContext sparkContext) {
		addJars(sparkContext);
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

		// Lazy-load the collection in as a DF,
		profiler.addTask("LOAD_MONGO_COLLECTION");
		Dataset<Row> mongoCollection = MongoSpark.load(sparkContext, readConfig).toDF();

		// SQL Select only _id, gis_join, features, and label columns, and discard the rest.
		// Then, rename the label column to "label", and features column to "features".
		Dataset<Row> selected = mongoCollection.select("_id", desiredColumns(requestCollection.getFeaturesList(),
				requestCollection.getLabel()))
				.withColumnRenamed(requestCollection.getLabel(), "label")
				.withColumnRenamed(requestCollection.getFeatures(0), "features");

		// SQL Filter by the GISJoins that they requested (i.e. WHERE gis_join IN ( value1, value2, value3 ) )
		Dataset<Row> gisDataset = selected.filter(selected.col("gis_join")
				.isInCollection(lrRequest.getGisJoinsList()));

		gisDataset.groupBy("gis_join").df().show();
		/*
		// Create map function for Map portion of Map Reduce
		MapFunction<Row, Tuple2<String, Double>> mapFunction = row -> new Tuple2<String, Double>(
				row.getAs("gis_join"), row.getAs("features")
		);

		// Create Encoder to convert JVM objects to Spark SQL representations
		Encoder<Tuple2<String, Double>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE());

		// Map the Rows to KV pairs where the key is the String GISJoin, and the value is the feature
		Dataset<Tuple2<String, Double>> keyValuePairs = gisDataset.map(mapFunction, encoder);

		KeyValueGroupedDataset<String, Double> kvPairs = gisDataset.groupByKey(mapFunction, encoder);
		*/



		//Dataset<Row> gisDataset = selected.filter(selected.col("gis_join").unary_$bang()
		//		.isInCollection(lrRequest.getGisJoinsList()));

		// Persist filtered data to memory

		//Dataset<Row> persistedCollection = gisDataset.checkpoint(true);
		//log.info(">>> mongoCollection Size: {}", readableBytes(SizeEstimator.estimate(persistedCollection)));

		profiler.completeTask("LOAD_MONGO_COLLECTION");

		/*

		// Build and run a model for each GISJoin in the request
		for (String gisJoin: lrRequest.getGisJoinsList()) {

			String modelTaskName = String.format("MODEL_GISJOIN_%s", gisJoin);
			profiler.addTask(modelTaskName);
			profiler.indent();

			LinearRegressionModelImpl model = new LinearRegressionModelImpl.LinearRegressionModelBuilder()
					.forMongoCollection(persistedCollection)
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



		// Unpersist collection and complete task

		 */
		//persistedCollection.unpersist(true);
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

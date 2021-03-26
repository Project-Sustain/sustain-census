/* ---------------------------------------------------------------------------------------------------------------------
 * LinearRegressionModel.java -
 *      Defines a generalized linear regression model that can be
 *      built and executed over a set of MongoDB documents.
 *
 * Author: Caleb Carlson
 * ------------------------------------------------------------------------------------------------------------------ */

package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SizeEstimator;
import org.sustain.util.Constants;
import org.sustain.util.Profiler;
import scala.collection.JavaConverters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.Seq;

import java.util.*;

/**
 * Provides an interface for building generalized Linear Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class LinearRegressionModelImpl {

    protected static final Logger log = LogManager.getLogger(LinearRegressionModelImpl.class);

    private JavaSparkContext sparkContext;
    private ReadConfig       mongoReadConfig;
    private Dataset<Row>     mongoCollection;
    private List<String>     features;
    private String           gisJoin, label, loss, solver;
    private Integer          aggregationDepth, maxIterations, totalIterations;
    private Double           elasticNetParam, epsilon, regularizationParam, convergenceTolerance, rmse, r2, intercept;
    private List<Double>     coefficients, objectiveHistory;
    private Boolean          fitIntercept, setStandardization;


    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private LinearRegressionModelImpl() {}


    public String getGisJoin() {
        return gisJoin;
    }

    public Double getRmse() {
        return rmse;
    }

    public Double getR2() {
        return r2;
    }

    public Double getIntercept() {
        return intercept;
    }

    public List<Double> getCoefficients() {
        return coefficients;
    }

    public List<Double> getObjectiveHistory() {
        return objectiveHistory;
    }

    public Integer getTotalIterations() {
        return totalIterations;
    }

    public void buildAndRunModel(Profiler profiler) {

        log.info(">>> Building model for GISJoin {}", this.gisJoin);

        // Filter collection by our GISJoin
        String filterTaskName = String.format("FILTER_GISJOIN_%s", this.gisJoin);
        profiler.addTask(filterTaskName);
        Dataset<Row> gisDataset = this.mongoCollection.filter(this.mongoCollection.col("gis_join").$eq$eq$eq(this.gisJoin))
                .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"
        profiler.completeTask(filterTaskName);

        // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
        String vectorTransformTaskName = String.format("VECTOR_TRANSFORM_%s", this.gisJoin);
        profiler.addTask(vectorTransformTaskName);
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(this.features.toArray(new String[0]))
                .setOutputCol("features");

        // Transform the gisDataset to have the new "features" column vector
        Dataset<Row> mergedDataset = vectorAssembler.transform(gisDataset);
        log.info(">>> mergedDataset Size: {}", SizeEstimator.estimate(mergedDataset));
        log.info(">>> mergedDataset explain():");

        /*
        mergedDataset.explain();
        profiler.completeTask(vectorTransformTaskName);

        // Create an MLLib Linear Regression object using user-specified parameters
        String lrCreateFitTaskName = String.format("LR_CREATE_FIT_%s", this.gisJoin);
        profiler.addTask(lrCreateFitTaskName);
        LinearRegression linearRegression = new LinearRegression()
                .setLoss(this.loss)
                .setSolver(this.solver)
                .setAggregationDepth(this.aggregationDepth)
                .setMaxIter(this.maxIterations)
                .setEpsilon(this.epsilon)
                .setElasticNetParam(this.elasticNetParam)
                .setRegParam(this.regularizationParam)
                .setTol(this.convergenceTolerance)
                .setFitIntercept(this.fitIntercept)
                .setStandardization(this.setStandardization);

        // Fit the dataset with the "features" and "label" columns
        LinearRegressionModel lrModel = linearRegression.fit(mergedDataset);
        profiler.completeTask(lrCreateFitTaskName);

        // Save training summary
        LinearRegressionTrainingSummary summary = lrModel.summary();

        this.coefficients = new ArrayList<>();
        double[] primitiveCoefficients = lrModel.coefficients().toArray();
        for (double d: primitiveCoefficients) {
            this.coefficients.add(d);
        }

        this.objectiveHistory = new ArrayList<>();
        double[] primitiveObjHistory = summary.objectiveHistory();
        for (double d: primitiveObjHistory) {
            this.objectiveHistory.add(d);
        }

        this.intercept = lrModel.intercept();
        this.totalIterations = summary.totalIterations();
        this.rmse = summary.rootMeanSquaredError();
        this.r2 = summary.r2();


         */
        log.info(">>> Finished building model for GISJoin {}", this.gisJoin);
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("SUSTAIN Linear Regression Model")
                .config("spark.mongodb.input.uri", String.format("mongodb://%s:%d", Constants.DB.HOST, Constants.DB.PORT))
                .config("spark.mongodb.input.database", Constants.DB.NAME)
                .config("spark.mongodb.input.collection", "maca_v2")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        ReadConfig readConfig = ReadConfig.create(sparkContext);

        LinearRegressionModelImpl lrModel = new LinearRegressionModelBuilder()
                .forMongoCollection(MongoSpark.load(sparkContext, readConfig).toDF())
                .forGISJoin("G0100290") // Cleburne County, Alabama
                .forFeatures(Collections.singletonList("singleton"))
                .forLabel("max_max_air_temperature")
                .withMaxIterations(100)
                .withEpsilon(1.35)
                .withTolerance(1E-7)
                .build();


        lrModel.buildAndRunModel(new Profiler());
        log.info("Executed LinearRegressionModelImpl.main() successfully");
        sparkContext.close();
    }

    /**
     * Builder class for the LinearRegressionModelImpl object.
     */
    public static class LinearRegressionModelBuilder implements ModelBuilder<LinearRegressionModelImpl> {

        private JavaSparkContext sparkContext;
        private ReadConfig       mongoReadConfig;
        private Dataset<Row>     mongoCollection;
        private List<String>     features;
        private String           gisJoin, label;

        // Model parameters and their defaults
        private String           loss="squaredError", solver="auto";
        private Integer          aggregationDepth=2, maxIterations=10;
        private Double           elasticNetParam=0.0, epsilon=1.35, regularizationParam=0.5, convergenceTolerance=1E-6;
        private Boolean          fitIntercept=true, setStandardization=true;

        public LinearRegressionModelBuilder forSparkContext(JavaSparkContext sparkContextReference) {
            this.sparkContext = sparkContextReference;
            return this;
        }

        public LinearRegressionModelBuilder forReadConfig(ReadConfig mongoReadConfig) {
            this.mongoReadConfig = mongoReadConfig;
            return this;
        }

        public LinearRegressionModelBuilder forMongoCollection(Dataset<Row> mongoCollection) {
            this.mongoCollection = mongoCollection;
            return this;
        }

        public LinearRegressionModelBuilder forGISJoin(String gisJoin) {
            this.gisJoin = gisJoin;
            return this;
        }

        public LinearRegressionModelBuilder forLabel(String label) {
            this.label = label;
            return this;
        }

        public LinearRegressionModelBuilder forFeatures(List<String> features) {
            this.features = features;
            return this;
        }

        public LinearRegressionModelBuilder withLoss(String loss) {
            if (!loss.isBlank()) {
                this.loss = loss;
            }
            return this;
        }

        public LinearRegressionModelBuilder withSolver(String solver) {
            if (!solver.isBlank()) {
                this.solver = solver;
            }
            return this;
        }

        public LinearRegressionModelBuilder withAggregationDepth(Integer aggregationDepth) {
            if (aggregationDepth != null && aggregationDepth >= 2 && aggregationDepth <= 10) {
                this.aggregationDepth = aggregationDepth;
            }
            return this;
        }

        public LinearRegressionModelBuilder withMaxIterations(Integer maxIterations) {
            if (maxIterations != null && maxIterations >= 0 && maxIterations < 100) {
                this.maxIterations = maxIterations;
            }
            return this;
        }

        public LinearRegressionModelBuilder withElasticNetParam(Double elasticNetParam) {
            if ((elasticNetParam != null) && elasticNetParam >= 0.0 && elasticNetParam <= 1.0 ) {
                this.elasticNetParam = elasticNetParam;
            }
            return this;
        }

        public LinearRegressionModelBuilder withEpsilon(Double epsilon) {
            if (epsilon != null && epsilon > 1.0 && epsilon <= 10.0) {
                this.epsilon = epsilon;
            }
            return this;
        }

        public LinearRegressionModelBuilder withRegularizationParam(Double regularizationParam) {
            if (regularizationParam != null && regularizationParam >= 0.0 && regularizationParam <= 10.0 ) {
                this.regularizationParam = regularizationParam;
            }
            return this;
        }

        public LinearRegressionModelBuilder withTolerance(Double convergenceTolerance) {
            if (convergenceTolerance != null && convergenceTolerance >= 0.0 && convergenceTolerance <= 10.0 )
            this.convergenceTolerance = convergenceTolerance;
            return this;
        }

        public LinearRegressionModelBuilder withFitIntercept(Boolean fitIntercept) {
            if (fitIntercept != null) {
                this.fitIntercept = fitIntercept;
            }
            return this;
        }

        public LinearRegressionModelBuilder withStandardization(Boolean setStandardization) {
            if (setStandardization != null) {
                this.setStandardization = setStandardization;
            }
            return this;
        }

        @Override
        public LinearRegressionModelImpl build() {
            LinearRegressionModelImpl model = new LinearRegressionModelImpl();
            model.sparkContext = this.sparkContext;
            model.mongoReadConfig = this.mongoReadConfig;
            model.mongoCollection = this.mongoCollection;
            model.gisJoin = this.gisJoin;
            model.features = this.features;
            model.label = this.label;
            model.loss = this.loss;
            model.solver = this.solver;
            model.aggregationDepth = this.aggregationDepth;
            model.maxIterations = this.maxIterations;
            model.elasticNetParam = this.elasticNetParam;
            model.epsilon = this.epsilon;
            model.regularizationParam = this.regularizationParam;
            model.convergenceTolerance = this.convergenceTolerance;
            model.fitIntercept = this.fitIntercept;
            model.setStandardization = this.setStandardization;
            return model;
        }
    }

}

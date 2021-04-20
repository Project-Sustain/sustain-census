/* ---------------------------------------------------------------------------------------------------------------------
 * SustainLinearRegression.java -
 *      Defines a generalized, serializable linear regression model that can be
 *      built and executed over a set of MongoDB documents. Can be passed to
 *      Spark Executors prior to being built.
 *
 * Author: Caleb Carlson
 * ------------------------------------------------------------------------------------------------------------------ */

package spark;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

import java.util.*;

/**
 * Provides an interface for building generalized Linear Regression
 * models on data pulled in using Mongo's Spark Connector. It is
 * Serializable in order to parallelize / send it to executors as an RDD.
 */
public class SustainLinearRegression implements SustainModel {

    //private Dataset<Row>     mongoCollection;
    private String           sparkMaster, mongoRouter, database, collection, feature, label, gisJoin, loss, solver;
    private Integer          aggregationDepth, maxIterations, totalIterations;
    private Double           elasticNetParam, epsilon, regularizationParam, convergenceTolerance, rmse, r2, intercept;
    private List<Double>     coefficients, objectiveHistory;
    private Boolean          fitIntercept, setStandardization;

    /**
     * Default constructor, made private so only the Builder class may access it.
     */
    private SustainLinearRegression() {}

    public String getSparkMaster() {
        return sparkMaster;
    }

    public String getMongoRouter() {
        return mongoRouter;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public String getFeature() {
        return feature;
    }

    public String getLabel() {
        return label;
    }

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

    protected SparkSession getOrCreateSparkSession() {
        // get or create SparkSession
        String hostname = System.getenv("HOSTNAME");
        return SparkSession.builder()
                .master(this.sparkMaster)
                .appName(String.format("Sustain Linear Regression - %s", hostname))
                .config("spark.mongodb.input.uri", String.format("%s/%s.%s",
                        this.mongoRouter, this.database, this.collection))
                .getOrCreate();
    }

    @Override
    public void trainModel() {

        // Create Spark Context
        SparkSession sparkSession = getOrCreateSparkSession();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // Lazy-load the collection in as a DF (Dataset<Row>), transform/process it, then persist it to be reused
        // by each linear regression model per GISJoin
        Dataset<Row> mongoCollection = MongoSpark.load(sparkContext).toDF();

        mongoCollection.show(10);

        // Make up fake values
        this.intercept = -1.23;
        this.totalIterations = 99;
        this.rmse = -1.23;
        this.r2 = -1.23;
        this.coefficients = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));
        this.objectiveHistory = new ArrayList<>(Arrays.asList(1.1, 2.2, 3.3));

        /*
        // Filter collection by our GISJoin
        Dataset<Row> gisDataset = this.mongoCollection.filter(
                this.mongoCollection.col("gis_join").$eq$eq$eq(this.gisJoin)
        );

        // Create an MLLib Linear Regression object using user-specified parameters
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
        LinearRegressionModel lrModel = linearRegression.fit(gisDataset);

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
    }

    /**
     * Builder class for the SustainLinearRegression object.
     */
    public static class SustainLinearRegressionBuilder implements ModelBuilder<SustainLinearRegression> {

        // Spark/mongo session/context information
        private String           sparkMaster, mongoRouter, database, collection, feature, label;
        //private Dataset<Row>     mongoCollection;
        private String           gisJoin;

        // Model parameters and their defaults
        private String           loss="squaredError", solver="auto";
        private Integer          aggregationDepth=2, maxIterations=10;
        private Double           elasticNetParam=0.0, epsilon=1.35, regularizationParam=0.5, convergenceTolerance=1E-6;
        private Boolean          fitIntercept=true, setStandardization=true;

        /*
        public SustainLinearRegressionBuilder forMongoCollection(Dataset<Row> mongoCollection) {
            this.mongoCollection = mongoCollection;
            return this;
        }
        */

        public SustainLinearRegressionBuilder forSparkMaster(String sparkMaster) {
            this.sparkMaster = sparkMaster;
            return this;
        }

        public SustainLinearRegressionBuilder forMongoRouter(String mongoRouter) {
            this.mongoRouter = mongoRouter;
            return this;
        }

        public SustainLinearRegressionBuilder forDatabase(String database) {
            this.database = database;
            return this;
        }

        public SustainLinearRegressionBuilder forCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public SustainLinearRegressionBuilder forFeature(String feature) {
            this.feature = feature;
            return this;
        }

        public SustainLinearRegressionBuilder forLabel(String label) {
            this.label = label;
            return this;
        }

        public SustainLinearRegressionBuilder forGISJoin(String gisJoin) {
            this.gisJoin = gisJoin;
            return this;
        }

        public SustainLinearRegressionBuilder withLoss(String loss) {
            if (!loss.isBlank()) {
                this.loss = loss;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withSolver(String solver) {
            if (!solver.isBlank()) {
                this.solver = solver;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withAggregationDepth(Integer aggregationDepth) {
            if (aggregationDepth != null && aggregationDepth >= 2 && aggregationDepth <= 10) {
                this.aggregationDepth = aggregationDepth;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withMaxIterations(Integer maxIterations) {
            if (maxIterations != null && maxIterations >= 0 && maxIterations < 100) {
                this.maxIterations = maxIterations;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withElasticNetParam(Double elasticNetParam) {
            if ((elasticNetParam != null) && elasticNetParam >= 0.0 && elasticNetParam <= 1.0 ) {
                this.elasticNetParam = elasticNetParam;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withEpsilon(Double epsilon) {
            if (epsilon != null && epsilon > 1.0 && epsilon <= 10.0) {
                this.epsilon = epsilon;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withRegularizationParam(Double regularizationParam) {
            if (regularizationParam != null && regularizationParam >= 0.0 && regularizationParam <= 10.0 ) {
                this.regularizationParam = regularizationParam;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withTolerance(Double convergenceTolerance) {
            if (convergenceTolerance != null && convergenceTolerance >= 0.0 && convergenceTolerance <= 10.0 )
                this.convergenceTolerance = convergenceTolerance;
            return this;
        }

        public SustainLinearRegressionBuilder withFitIntercept(Boolean fitIntercept) {
            if (fitIntercept != null) {
                this.fitIntercept = fitIntercept;
            }
            return this;
        }

        public SustainLinearRegressionBuilder withStandardization(Boolean setStandardization) {
            if (setStandardization != null) {
                this.setStandardization = setStandardization;
            }
            return this;
        }

        @Override
        public SustainLinearRegression build() {
            SustainLinearRegression model = new SustainLinearRegression();
            //model.mongoCollection = this.mongoCollection;
            model.sparkMaster = this.sparkMaster;
            model.mongoRouter = this.mongoRouter;
            model.database = this.database;
            model.collection = this.collection;
            model.feature = this.feature;
            model.label = this.label;
            model.gisJoin = this.gisJoin;
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

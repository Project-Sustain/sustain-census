package org.sustain.analytics;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.ModelBuilder;
import org.sustain.util.Constants;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.avg;

public class RollingAverageStatImpl {
    protected static final Logger log = LogManager.getLogger(RollingAverageStatImpl.class);

    private JavaSparkContext sparkContext;
    private ReadConfig       mongoReadConfig;
    private Dataset<Row>     mongoCollection;
    private List<String>     features;
    private String           gisJoin;

    private RollingAverageStatImpl(){}

    /**
     * Compiles a List<String> of column names we desire from the loaded collection, using the features String array.
     * @return A Scala Seq<String> of desired column names.
     */
    private Seq<String> desiredColumns() {
        List<String> cols = new ArrayList<>();
        cols.add("GISJOIN");
        cols.addAll(this.features);
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
     * Calculates the average of the 'cases' column from covid_county_formatted given a valid UTC date range
     * @param start Starting UTC date string
     * @param end Ending UTC date string
     */
    private void calculateAverageOverDates(String start, String end){
        String isoDatePattern = "yyyy-MM-dd HH:mm:ss.SSS ZZZ";
        Date formattedStart = null;
        Date formattedEnd = null;
        try {
            formattedStart = new SimpleDateFormat(isoDatePattern).parse(start);
            formattedEnd = new SimpleDateFormat(isoDatePattern).parse(end);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        java.sql.Date sqlStart = new java.sql.Date(formattedStart.getTime());
        java.sql.Date sqlEnd = new java.sql.Date(formattedEnd.getTime());

        // Lazy-load the collection in as a DF
        Dataset<Row> collection = this.mongoCollection;
        log.info("Loaded data frame");

        // Select just the columns we want, discard the rest
        Dataset<Row> selected = collection.select("_id", desiredColumns());
        log.info("Filtered desired columns");

        //Filter on GIS-Join to get proper location
        Dataset<Row> gisDataset = selected.filter(selected.col("GISJOIN").$eq$eq$eq(gisJoin));
        log.info("Filtered by county");

        //Filter on given date range
        Dataset<Row> dateFiltered = gisDataset.filter(gisDataset.col("date")
                .$greater$eq(sqlStart)
                .$amp$amp(gisDataset.col("date")
                        .$less$eq(sqlEnd)));
        log.info("Filtered by daterange");

        Dataset<Row> meanData = dateFiltered
                .agg(avg("cases"));
        log.info("Calculated average");

        meanData.show();
    }

    public static void main(String[] args){
        SparkSession sparkSession = SparkSession.builder()
                .master(Constants.Spark.MASTER)
                .appName("SUSTAIN Rolling Average Calc")
                .config("spark.mongodb.input.uri", String.format("mongodb://%s:%d",
                        Constants.DB.HOST,
                        Constants.DB.PORT)) // mongodb://lattice-46:27017
                .config("spark.mongodb.input.database", Constants.DB.NAME) // sustaindb
                .config("spark.mongodb.input.collection", "covid_county_formatted") // future_heat
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        ReadConfig readConfig = ReadConfig.create(sparkContext);

        String[] jarPaths = {
                "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
                "build/libs/spark-core_2.12-3.0.1.jar",
                "build/libs/spark-sql_2.12-3.0.1.jar",
                "build/libs/bson-4.0.5.jar",
                "build/libs/mongo-java-driver-3.12.5.jar"
        };

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkContext.addJar(jar);
        }


        RollingAverageStatImpl raStat = new RollingAverageStatBuilder()
                .forMongoCollection(MongoSpark.load(sparkContext, readConfig).toDF())
                .forGISJoin("G0600850")//Santa Clara, CA
                .forFeatures(Arrays.asList("date", "cases"))
                .build();

        raStat.calculateAverageOverDates("2020-01-25 00:00:00.000 UTC","2020-02-04 00:00:00.000 UTC");
    }

    public static class RollingAverageStatBuilder implements ModelBuilder<RollingAverageStatImpl> {
        private JavaSparkContext sparkContext;
        private ReadConfig mongoReadConfig;

        private Dataset<Row> mongoCollection;

        private String gisJoin;
        private List<String> features;

        public RollingAverageStatBuilder forSparkContext(JavaSparkContext sparkContextRef){
            this.sparkContext = sparkContextRef;
            return this;
        }

        public RollingAverageStatBuilder forReadConfig(ReadConfig readConfigRef){
            this.mongoReadConfig = readConfigRef;
            return this;
        }

        public RollingAverageStatBuilder forMongoCollection(Dataset<Row> mongoCollection){
            this.mongoCollection = mongoCollection;
            return this;
        }

        public RollingAverageStatBuilder forGISJoin(String gisJoin) {
            this.gisJoin = gisJoin;
            return this;
        }

        public RollingAverageStatBuilder forFeatures(List<String> features) {
            this.features = features;
            return this;
        }

        @Override
        public RollingAverageStatImpl build() {
            RollingAverageStatImpl model = new RollingAverageStatImpl();
            model.sparkContext = this.sparkContext;
            model.mongoReadConfig = this.mongoReadConfig;
            model.mongoCollection = this.mongoCollection;
            model.gisJoin = this.gisJoin;
            model.features = this.features;
            return model;
        }
    }
}


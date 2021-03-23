package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.sustain.*;
import org.sustain.util.Constants;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Created by laksheenmendis on 3/22/21 at 12:44 AM
 */
public class KNNSearchQueryHandler extends GrpcSparkHandler<ModelRequest, ModelResponse> implements SparkTask<Boolean> {

    private static final Logger log = LogManager.getFormatterLogger(KNNSearchQueryHandler.class);
    private Dataset<Row> selectedFeatures;
    private VectorAssembler assembler;
    private MinMaxScalerModel scalerModel;

    public KNNSearchQueryHandler(ModelRequest request, StreamObserver<ModelResponse> responseObserver, SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public boolean isValid(ModelRequest modelRequest) {
        // TODO: Implement
        return true;
    }

    @Override
    public Boolean execute(JavaSparkContext sparkContext) throws Exception {

        kNNSearch(sparkContext);
        return true;
    }

    @Override
    public void handleRequest() {
        this.logRequest(request);

        try {
            // Submit task to Spark Manager
            Future<Boolean> future =
                    this.sparkManager.submit(this, "kNN-query");

            // Wait for task to complete
            future.get();
        } catch (Exception e) {
            log.error("Failed to evaluate kNN query", e);
            responseObserver.onError(e);
        }

    }

    private void kNNSearch(JavaSparkContext sparkContext) {
        Dataset<Row> featureDF = preprocessAndGetFeatureDF(sparkContext);
        StructType schema = selectedFeatures.schema();

        List<Integer> queryItem = getFeatureValues();
        Row row = RowFactory.create(queryItem.toArray());

        List<Row> rowList = new ArrayList<>();
        rowList.add(row);

        SQLContext sqlContext = new SQLContext(sparkContext);

        // convert the query with the same min-max normalization
        Dataset<Row> query = sqlContext.createDataFrame(rowList, schema);
        Dataset<Row> transformedQuery = assembler.transform(query);
        transformedQuery = scalerModel.transform(transformedQuery);
        transformedQuery = transformedQuery.drop("features");
        transformedQuery = transformedQuery.withColumnRenamed("normalized_features", "features1");

        log.info("Query Dataframe after min-max normalization");
        transformedQuery.show(1);
        JavaRDD<Row> featureJavaRDD = featureDF.toJavaRDD();

        JavaPairRDD<Double, String> distancePairRDD = transformedQuery.toJavaRDD().cartesian(featureJavaRDD).mapToPair((PairFunction<Tuple2<Row, Row>, Double, String>) rowRowTuple2 -> {
            Vector features1 = rowRowTuple2._1.getAs("features1");
            Vector data = rowRowTuple2._2.getAs("features");

            double sqdist;
            try{
                sqdist = calculateDistance(features1, data);
            }
            catch (Exception e)
            {
                throw new Exception(e.getMessage());
            }
            return new Tuple2<>(sqdist, rowRowTuple2._2.getAs("GISJOIN"));
        });

        int k = this.request.getKNNSearchRequest().getKValue();
        // sort by distance, and extract the values (i.e. object ids), and take k out of it
        List<String> topKIDs = distancePairRDD.sortByKey().values().take(k);

        List<Tuple2<String, Integer>> kNNresults = sparkContext.parallelize(topKIDs).zipWithIndex().mapValues(x -> x.intValue() + 1).collect();

        for (Tuple2<String, Integer> result: kNNresults) {
            responseObserver.onNext(ModelResponse.newBuilder()
                    .setKNNSearchResponse(
                            KNNSearchResponse.newBuilder()
                                    .setGisJoin(result._1)
                                    .setRank(result._2)
                                    .build()
                    ).build()
            );
        }
    }

    private double calculateDistance(Vector v1, Vector v2) throws Exception {

        DistanceMetric distanceMetric = this.request.getKNNSearchRequest().getDistanceMetric();
        double sqdist = -1d;
        switch (distanceMetric)
        {
            case Euclidean:
                sqdist = Vectors.sqdist(v1, v2);
                break;
            case Cosine:
                sqdist = cosineDistance(v1, v2);
                break;
            case Minkowski:
                sqdist = minkowskiDistance(v1, v2);
                break;
            case UNRECOGNIZED:
                sqdist = Vectors.sqdist(v1, v2);
                break;
        }
        return sqdist;
    }

    private double minkowskiDistance(Vector v1, Vector v2) throws Exception {

        int p = 3;  //default value for p
        if(this.request.getKNNSearchRequest().getPForMinkowski() > 0)
            p = this.request.getKNNSearchRequest().getPForMinkowski();

        int size = v1.size();
        if(size != v2.size())
        {
            log.error("Vector sizes are not equal. Cannot calculate Minkowski Distance");
            throw new Exception("Vector sizes are not equal. Cannot calculate Minkowski Distance");
        }

        double[] v1_arr = v1.toArray();
        double[] v2_arr = v2.toArray();

        double sum = 0d;
        int i=0;
        while(i < size)
        {
            sum += Math.pow(Math.abs(v1_arr[i] - v2_arr[i]) ,p);
            i++;
        }
        return Math.pow(sum, (double) 1/p);
    }

    private double cosineDistance(Vector v1, Vector v2) {
        double dotProduct = v1.dot(v2);
        double crossProduct = Vectors.norm(v1, 2) * Vectors.norm(v2, 2);

        return dotProduct/crossProduct;
    }

    private Dataset<Row> preprocessAndGetFeatureDF(JavaSparkContext sparkContext) {

        // whether state, county, tract or block
        String resolution = request.getKNNSearchRequest().getResolution().toString().toLowerCase();

        // Initialize mongodb read configuration
        HashMap<String, String> readOverrides = new HashMap();
        readOverrides.put("spark.mongodb.input.collection", resolution + "_stats");
        readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
        readOverrides.put("spark.mongodb.input.uri",
                "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

        ReadConfig readConfig = ReadConfig.create(sparkContext);
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();
        List<String> featuresList = getFeatureNames();
        Seq<String> features = convertListToSeq(featuresList);

        selectedFeatures = collection.select(Constants.GIS_JOIN, features);

        // Dropping rows with null values
        selectedFeatures = selectedFeatures.na().drop();

        // Assembling
        assembler = new VectorAssembler().setInputCols(featuresList.toArray(new String[0])).setOutputCol("features");
        Dataset<Row> featureDF = assembler.transform(selectedFeatures);
        featureDF.show(10);

        // Scaling
        log.info("Normalizing features");
        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("normalized_features");
        scalerModel = scaler.fit(featureDF);

        featureDF = scalerModel.transform(featureDF);
        featureDF = featureDF.drop("features");
        featureDF = featureDF.withColumnRenamed("normalized_features", "features");

        log.info("Dataframe after min-max normalization");
        featureDF.show(10);

        return featureDF;
    }

    private List<String> getFeatureNames() {
        List<String> featuresList = new ArrayList<>();

        Dictionary queryMap = this.request.getKNNSearchRequest().getQueryMap();
        featuresList.addAll(queryMap.getPairsMap().keySet());

        return featuresList;
    }

    private List<Integer> getFeatureValues() {
        List<Integer> valuesList = new ArrayList<>();

        // need to add a null value to compensate for GISJOIN field
        valuesList.add(null);

        Dictionary queryMap = this.request.getKNNSearchRequest().getQueryMap();
        valuesList.addAll(queryMap.getPairsMap().values());

        return valuesList;
    }

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }
}

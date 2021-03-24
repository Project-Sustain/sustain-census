package org.sustain.handlers;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import io.grpc.stub.StreamObserver;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SQLContext;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sustain.CountResponse;
import org.sustain.CountRequest;
import org.sustain.SparkManager;
import org.sustain.SparkTask;
import org.sustain.util.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class CountQueryHandler extends GrpcSparkHandler<CountRequest, CountResponse>
        implements SparkTask<Boolean> {
    protected static final Logger log =
        LoggerFactory.getLogger(CountQueryHandler.class);

    public CountQueryHandler(CountRequest request,
            StreamObserver<CountResponse> responseObserver,
            SparkManager sparkManager) {
        super(request, responseObserver, sparkManager);
    }

    @Override
    public void handleRequest() {
        try {
            // Submit task to Spark Manager
            Future<Boolean> future =
                this.sparkManager.submit(this, "count-query");

            // Wait for task to complete
            future.get();

            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to evaluate query", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public Boolean execute(JavaSparkContext sparkContext, SQLContext sqlContext) throws Exception {
        // initialize ReadConfig
        Map<String, String> readOverrides = new HashMap();
        readOverrides.put("spark.mongodb.input.collection",
			request.getCollection());
        readOverrides.put("spark.mongodb.input.database", Constants.DB.NAME);
        readOverrides.put("spark.mongodb.input.uri",
            "mongodb://" + Constants.DB.HOST + ":" + Constants.DB.PORT);

        ReadConfig readConfig = 
            ReadConfig.create(sparkContext.getConf(), readOverrides);

        // load RDD
        JavaMongoRDD<Document> rdd =
            MongoSpark.load(sparkContext, readConfig);

        // perform count evaluation
        long count = rdd.count();

        // initailize response
        CountResponse response = CountResponse.newBuilder()
            .setCount(count)
            .build();

        // send response
        responseObserver.onNext(response);

        return true;
    }
}

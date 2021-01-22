package org.sustain.modeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.LinearRegressionRequest;
import org.sustain.LinearRegressionResponse;

public class LinearRegressionQueryHandler {
    private static final Logger log = LogManager.getLogger(LinearRegressionQueryHandler.class);
    private final LinearRegressionRequest request;
    private final StreamObserver<LinearRegressionResponse> responseObserver;


    public LinearRegressionQueryHandler(LinearRegressionRequest request, StreamObserver<LinearRegressionResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleQuery() {
        SparkJob sparkJob = new SparkJob();
        String jsonResponse = sparkJob.createSparkJob(request.getRequest());
        System.out.println(jsonResponse);

        responseObserver.onNext(LinearRegressionResponse.newBuilder().setResults(jsonResponse).build());
        responseObserver.onCompleted();
    }


}

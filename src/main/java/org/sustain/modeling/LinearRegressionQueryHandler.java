package org.sustain.modeling;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.LinearRegressionRequest;
import org.sustain.LinearRegressionResponse;
import org.sustain.dataModeling.ModelQueryHandler;

public class LinearRegressionQueryHandler {
    private static final Logger log = LogManager.getLogger(ModelQueryHandler.class);
    private final LinearRegressionRequest request;
    private final StreamObserver<LinearRegressionResponse> responseObserver;


    public LinearRegressionQueryHandler(LinearRegressionRequest request, StreamObserver<LinearRegressionResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleQuery() {
        System.out.println(request.getRequest());
    }


}

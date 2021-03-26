package org.sustain.server;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CompoundRequest;
import org.sustain.CompoundResponse;
import org.sustain.CountRequest;
import org.sustain.CountResponse;
import org.sustain.DirectRequest;
import org.sustain.DirectResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.ModelType;
import org.sustain.SlidingWindowRequest;
import org.sustain.SlidingWindowResponse;
import org.sustain.SparkManager;
import org.sustain.SustainGrpc;
import org.sustain.handlers.ClusteringQueryHandler;
import org.sustain.handlers.CompoundQueryHandler;
import org.sustain.handlers.CountQueryHandler;
import org.sustain.handlers.GrpcHandler;
import org.sustain.handlers.RegressionQueryHandler;
import org.sustain.handlers.DirectQueryHandler;
import org.sustain.handlers.EnsembleQueryHandler;
import org.sustain.handlers.GrpcHandler;
import org.sustain.handlers.RegressionQueryHandler;
import org.sustain.handlers.SlidingWindowQueryHandler;

public class SustainService extends SustainGrpc.SustainImplBase {
    private static final Logger log = LogManager.getLogger(SustainService.class);
    private SparkManager sparkManager;

    public SustainService(SparkManager sparkManager) {
        this.sparkManager = sparkManager;
    }

    @Override
    public void slidingWindowQuery(SlidingWindowRequest request,
                                   StreamObserver<SlidingWindowResponse> responseObserver) {
        SlidingWindowQueryHandler handler = new SlidingWindowQueryHandler(request, responseObserver);
        log.info("Received a Sliding Window Query Request");
        handler.handleRequest();
    }

    @Override
    public void modelQuery(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {

        GrpcHandler handler;
        ModelType type = request.getType();
        switch (type) {
            case LINEAR_REGRESSION:
                log.info("Received a Linear Regression Model request");
                handler = new RegressionQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case K_MEANS_CLUSTERING:
                log.info("Received a K-Means Clustering Model request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case BISECTING_K_MEANS:
                log.info("Received a Bisecting K-Means Model Request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case GAUSSIAN_MIXTURE:
                log.info("Received a Gaussian Mixture Request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case R_FOREST_REGRESSION:
                log.info("Received a Random Forest Regression Model request");
                handler = new EnsembleQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case G_BOOST_REGRESSION:
                log.info("Received a Gradient Boost Regression Model request");
                handler = new EnsembleQueryHandler(request, responseObserver, this.sparkManager);
                break;
            case LATENT_DIRICHLET_ALLOCATION:
                log.info("Received a Latent Dirichlet Allocation Request");
                handler = new ClusteringQueryHandler(request, responseObserver, this.sparkManager);
                break;
            default:
                responseObserver.onError(new Exception("Invalid Model Type"));
                return;
        }

        handler.handleRequest();
    }

    @Override
    public void compoundQuery(CompoundRequest request, StreamObserver<CompoundResponse> responseObserver) {
        GrpcHandler<CompoundRequest, CompoundResponse> handler = new CompoundQueryHandler(request,
            responseObserver);
        handler.handleRequest();
    }

    @Override
    public void countQuery(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        GrpcHandler<CountRequest, CountResponse> handler =
            new CountQueryHandler(request, responseObserver, this.sparkManager);
        handler.handleRequest();
    }

    @Override
    public void directQuery(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
        GrpcHandler<DirectRequest, DirectResponse> handler = new DirectQueryHandler(request, responseObserver);
        handler.handleRequest();
    }

    /**
     * An example RPC method used to sanity-test the gRPC server manually, or unit-test it with JUnit.
     *
     * @param request          DirectRequest object containing a collection and query request.
     * @param responseObserver Response Stream for streaming back results.
     */
    @Override
    public void echoQuery(DirectRequest request, StreamObserver<DirectResponse> responseObserver) {
        log.info("RPC method echoQuery() invoked; returning request query body");
        DirectResponse echoResponse = DirectResponse.newBuilder()
            .setData(StringEscapeUtils.unescapeJavaScript(request.getQuery()))
            .build();
        responseObserver.onNext(echoResponse);
        responseObserver.onCompleted();
    }
}

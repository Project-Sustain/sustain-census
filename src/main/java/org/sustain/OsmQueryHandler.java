package org.sustain;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.OsmRequest;
import org.sustain.census.OsmResponse;
import org.sustain.census.controller.mongodb.OsmController;

import java.util.concurrent.LinkedBlockingQueue;

public class OsmQueryHandler {
    private static final Logger log = LogManager.getLogger(OsmQueryHandler.class);

    private final OsmRequest request;
    private final StreamObserver<OsmResponse> responseObserver;
    private boolean fetchingCompleted = false;

    public OsmQueryHandler(OsmRequest request, StreamObserver<OsmResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleOsmQuery() {
        OsmRequest.Dataset dataset = request.getDataset();
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        new StreamWriter(queue, responseObserver).start();
        switch (dataset) {
            // query all OSM datasets
            case ALL:
                OsmController.getOsmData(request, OsmRequest.Dataset.LINES, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.MULTI_LINES, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.POINTS, queue);
                //OsmController.getOsmData(request, OsmRequest.Dataset.MULTI_POLYGONS, queue);
                OsmController.getOsmData(request, OsmRequest.Dataset.OTHER, queue);

                fetchingCompleted = true;
                return;
            case UNRECOGNIZED:
                log.warn("Invalid OSM dataset");
        }

        // not ALL, query a single OSM dataset
        OsmController.getOsmData(request, dataset, queue);

        fetchingCompleted = true;
    }

    private class StreamWriter extends Thread {
        private volatile LinkedBlockingQueue<String> data;
        private StreamObserver<OsmResponse> responseObserver;

        public StreamWriter(LinkedBlockingQueue<String> data, StreamObserver<OsmResponse> responseObserver) {
            this.data = data;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            log.info("Starting StreamWriter thread");
            while (!fetchingCompleted) {
                // log.info("Queue size: " + data.size());
                if (data.size() > 0) {
                    String datum = data.remove();
                    responseObserver.onNext(OsmResponse.newBuilder().setResponse(datum).build());
                }
            }

            // if there if any data remaining in the queue after fetching is completed
            for (String datum : data) {
                responseObserver.onNext(OsmResponse.newBuilder().setResponse(datum).build());
            }

            responseObserver.onCompleted();
        }
    }
}

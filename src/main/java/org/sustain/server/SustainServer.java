package org.sustain.server;

import com.mongodb.client.model.geojson.Geometry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CensusRequest;
import org.sustain.CensusResponse;
import org.sustain.CompoundRequest;
import org.sustain.CompoundResponse;
import org.sustain.DatasetRequest;
import org.sustain.DatasetResponse;
import org.sustain.ModelRequest;
import org.sustain.ModelResponse;
import org.sustain.OsmRequest;
import org.sustain.OsmResponse;
import org.sustain.Predicate;
import org.sustain.SpatialOp;
import org.sustain.SustainGrpc;
import org.sustain.SviRequest;
import org.sustain.SviResponse;
import org.sustain.census.CensusQueryHandler;
import org.sustain.census.controller.SpatialQueryUtil;
import org.sustain.dataModeling.ModelQueryHandler;
import org.sustain.openStreetMaps.OsmQueryHandler;
import org.sustain.otherDatasets.DatasetQueryHandler;
import org.sustain.querier.CompoundQueryHandler;
import org.sustain.svi.SviController;
import org.sustain.util.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.sustain.census.controller.SpatialQueryUtil.getGeometryFromGeoJson;


public class SustainServer {
    private static final Logger log = LogManager.getLogger(SustainServer.class);

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final SustainServer server = new SustainServer();
        server.start();
        server.blockUntilShutdown();
    }


    public void start() throws IOException {
        final int port = Constants.Server.PORT;
        server = ServerBuilder.forPort(port)
                .addService(new SustainService())
                .build().start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    SustainServer.this.stop();
                } catch (InterruptedException e) {
                    log.error("Error in stopping the server");
                    e.printStackTrace();
                }
                log.warn("Server is shutting down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void shutdownNow() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    // Server implementation
    static class SustainService extends SustainGrpc.SustainImplBase {
        @Override
        public void censusQuery(CensusRequest request, StreamObserver<CensusResponse> responseObserver) {
            CensusQueryHandler handler = new CensusQueryHandler(request, responseObserver);
            handler.handleCensusQuery();
        }

        @Override
        public void modelQuery(ModelRequest request, StreamObserver<ModelResponse> responseObserver) {
            ModelQueryHandler handler = new ModelQueryHandler(request, responseObserver);
        }

        @Override
        public void sviQuery(SviRequest request, StreamObserver<SviResponse> responseObserver) {
            Geometry geometry = getGeometryFromGeoJson(request.getRequestGeoJson());
            SpatialOp spatialOp = request.getSpatialOp();
            log.info("SVI Query{spatialOp: " + spatialOp + "}");
            HashMap<String, String> geos = new HashMap<>();
            switch (spatialOp) {
                case GeoWithin:
                    geos = SpatialQueryUtil.findGeoWithin(Constants.GeoJsonCollections.TRACTS_GEO,
                            geometry);
                    break;
                case GeoIntersects:
                    geos = SpatialQueryUtil.findGeoIntersects(Constants.GeoJsonCollections.TRACTS_GEO,
                            geometry);
                    break;
                case UNRECOGNIZED:
                    log.warn("Invalid spatial op: " + spatialOp);
            }
            ArrayList<String> gisJoins = new ArrayList<>(geos.keySet());
            for (String gisJoin : gisJoins) {
                String svi = SviController.getSviByGisJoin(gisJoin);
                if (svi != null) {
                    responseObserver.onNext(SviResponse.newBuilder()
                            .setData(svi)
                            .setResponseGeoJson(geos.get(gisJoin))
                            .build());
                }
            }

            responseObserver.onCompleted();
        }

        /**
         * Determine if a given value is valid after comparing it with the actual value using the\
         * given comparison Operator
         *
         * @param comparisonOp    comparison operator from gRPC request
         * @param value           value returned by the census data record
         * @param comparisonValue comparisonValue from gRPC request
         * @return comparison of 'value' to 'comparisonValue'
         */
        boolean compareValueWithInputValue(Predicate.ComparisonOperator comparisonOp, Double value,
                                           Double comparisonValue) {
            switch (comparisonOp) {
                case EQUAL:
                    return value.equals(comparisonValue);
                case GREATER_THAN_OR_EQUAL:
                    return value >= comparisonValue;
                case LESS_THAN:
                    return value < comparisonValue;
                case LESS_THAN_OR_EQUAL:
                    return value <= comparisonValue;
                case GREATER_THAN:
                    return value > comparisonValue;
                case UNRECOGNIZED:
                    log.warn("Unknown comparison operator");
                    return false;
            }
            return false;
        }

        /**
         * Execute a TargetedQuery - return geographical areas that satisfy a given value range of a census feature
         * Example 1: Retrieve all counties where (population >= 1,000,000)
         * Example 2: Retrieve all tracts where (median household income < $50,000/year)
         */
        /*
        @Override
        public void executeTargetedCensusQuery(TargetedCensusRequest request,
                                               StreamObserver<TargetedCensusResponse> responseObserver) {
            Predicate predicate = request.getPredicate();
            double comparisonValue = predicate.getComparisonValue();
            Predicate.ComparisonOperator comparisonOp = predicate.getComparisonOp();
            CensusFeature censusFeature = predicate.getCensusFeature();
            Decade _decade = predicate.getDecade();
            String resolution = Constants.TARGET_RESOLUTIONS.get(request.getResolution());

            String decade = Constants.DECADES.get(_decade);
            HashMap<String, GeoJson> geoJsonMap = getGeoList(request.getRequestGeoJson(), resolution,
                    request.getSpatialOp());

            try {
                switch (censusFeature) {
                    case TotalPopulation:
                        String comparisonField = decade + "_" + Constants.CensusFeatures.TOTAL_POPULATION;
                        for (GeoJson geoJson : geoList) {
                            String populationResult =
                                    PopulationController.getTotalPopulationResults(resolution,
                                            geoJson.getProperties().getGisJoin());
                            double value =
                                    JsonParser.parseString(populationResult).getAsJsonObject().get(comparisonField)
                                            .getAsDouble();
                            boolean valid = compareValueWithInputValue(comparisonOp, value, comparisonValue);

                            if (valid) {
                                SpatialResponse response = SpatialResponse.newBuilder()
                                        .setData(populationResult)
                                        .setResponseGeoJson(geoJson.toJson())
                                        .build();
                                responseObserver.onNext(TargetedCensusResponse.newBuilder().setSpatialResponse(response)
                                        .build());
                            }
                        }   // end of geoJson loop

                        responseObserver.onCompleted();
                        break;
                    case MedianHouseholdIncome:
                        HashMap<String, String> incomeResults = IncomeController.fetchTargetedInfo(decade,
                                resolution, comparisonOp, comparisonValue);
                        break;
                    case Race:
                        break;
                    case UNRECOGNIZED:
                        log.warn("Invalid Census censusFeature requested");
                        break;
                }   // end of census-feature switch
            } catch (Exception e) {
                log.error(e);
                e.printStackTrace();
            }
        }
        */
        @Override
        public void osmQuery(OsmRequest request, StreamObserver<OsmResponse> responseObserver) {
            OsmQueryHandler handler = new OsmQueryHandler(request, responseObserver);
            handler.handleOsmQuery();
        }

        @Override
        public void datasetQuery(DatasetRequest request, StreamObserver<DatasetResponse> responseObserver) {
            DatasetQueryHandler handler = new DatasetQueryHandler(request, responseObserver);
            handler.handleDatasetQuery();
        }

        @Override
        public void compoundQuery(CompoundRequest request, StreamObserver<CompoundResponse> responseObserver) {
            CompoundQueryHandler handler = new CompoundQueryHandler(responseObserver);
            handler.handleCompoundQuery(request, true);
        }

    }   // end of Server implementation
}

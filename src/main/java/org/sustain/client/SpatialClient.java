package org.sustain.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.*;
import org.sustain.util.Constants;
import org.sustain.util.SampleGeoJson;

import java.util.Iterator;

public class SpatialClient {
    private static final Logger log = LogManager.getLogger(SpatialClient.class);

    private SustainGrpc.SustainBlockingStub sustainBlockingStub;

    public SpatialClient() {
        String target = Constants.Server.HOST + ":" + 50051;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        logEnvironment();
        SustainGrpc.SustainBlockingStub sustainBlockingStub = new SpatialClient().getSustainBlockingStub();
        exampleLinearRegressionQuery(sustainBlockingStub);
        //exampleSpatialQuery(sustainBlockingStub, geoJson);
        //exampleTargetedQuery(sustainBlockingStub, geoJson);
        //exampleOsmQuery(sustainBlockingStub, SampleGeoJson.FORT_COLLINS);
        //exampleDatasetQuery(DatasetRequest.Dataset.FIRE_STATIONS, sustainBlockingStub, SampleGeoJson.MULTIPLE_STATES);
        //exampleCensusQuery(CensusFeature.TotalPopulation, CensusResolution.County, sustainBlockingStub,
        //        SampleGeoJson.COLORADO);
        //exampleSviQuery(SampleGeoJson.COLORADO, SpatialOp.GeoIntersects, sustainBlockingStub);
    }

    // Logs the environment variables that the server was started with.
    public static void logEnvironment() {
        log.info("--- Server Environment ---");
        log.info("SERVER_HOST: " + Constants.Server.HOST);
        log.info("SERVER_PORT: " + Constants.Server.PORT);
        log.info("--- Database Environment ---");
        log.info("DB_HOST: " + Constants.DB.HOST);
        log.info("DB_PORT: " + Constants.DB.PORT);
        log.info("DB_NAME: " + Constants.DB.NAME);
        log.info("DB_USERNAME: " + Constants.DB.USERNAME);
        log.info("DB_PASSWORD: " + Constants.DB.PASSWORD);
    }

    private static void exampleDatasetQuery(DatasetRequest.Dataset dataset,
                                            SustainGrpc.SustainBlockingStub sustainBlockingStub, String geoJson) {
        DatasetRequest request = DatasetRequest.newBuilder()
                .setDataset(dataset)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();
        Iterator<DatasetResponse> datasetResponseIterator = sustainBlockingStub.datasetQuery(request);
        int count = 0;
        while (datasetResponseIterator.hasNext()) {
            DatasetResponse response = datasetResponseIterator.next();
            count++;
            log.info(response.getResponse() + "\n");
        }

        log.info("Count: " + count);
    }

    private static void exampleSviQuery(String geoJson, SpatialOp spatialOp,
                                        SustainGrpc.SustainBlockingStub sustainBlockingStub) {
        SviRequest request = SviRequest.newBuilder()
                .setRequestGeoJson(geoJson)
                .setSpatialOp(spatialOp)
                .build();

        Iterator<SviResponse> responseIterator = sustainBlockingStub.sviQuery(request);
        int count = 0;
        while (responseIterator.hasNext()) {
            SviResponse response = responseIterator.next();
            count++;
            log.info(response.getData());
            //log.info(response.getResponseGeoJson());
            System.out.println();
        }
        log.info("Count: " + count);
    }

    private static void exampleOsmQuery(SustainGrpc.SustainBlockingStub censusBlockingStub, String geoJson) {
        OsmRequest request = OsmRequest.newBuilder()
                .setDataset(OsmRequest.Dataset.ALL)
                .setSpatialOp(SpatialOp.GeoWithin)
                // .addRequestParams(OsmRequest.OsmRequestParam.newBuilder()
                //         .setKey("properties.highway")
                //         .setValue("primary"))
                // .addRequestParams(OsmRequest.OsmRequestParam.newBuilder()
                //         .setKey("properties.highway")
                //         .setValue("residential"))
                .setRequestGeoJson(geoJson).build();

        Iterator<OsmResponse> osmResponseIterator = censusBlockingStub.osmQuery(request);
        int count = 0;
        while (osmResponseIterator.hasNext()) {
            OsmResponse response = osmResponseIterator.next();
            count++;
            log.info(response.getResponse() + "\n");
        }

        log.info("Count: " + count);
    }

    private static void exampleCensusQuery(CensusFeature censusFeature, CensusResolution censusResolution,
                                           SustainGrpc.SustainBlockingStub censusBlockingStub, String geoJson) {
        CensusRequest request = CensusRequest.newBuilder()
                .setCensusFeature(censusFeature)
                .setCensusResolution(censusResolution)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        int count = 0;
        Iterator<CensusResponse> CensusResponseIterator = censusBlockingStub.censusQuery(request);
        while (CensusResponseIterator.hasNext()) {
            CensusResponse response = CensusResponseIterator.next();
            String data = response.getData();
            String responseGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
            count++;
        }
        log.info("Count: " + count);
    }

    private static void exampleTargetedQuery(SustainGrpc.SustainBlockingStub censusBlockingStub, String geoJson) {
        TargetedCensusRequest request = TargetedCensusRequest.newBuilder()
                .setResolution(CensusResolution.Tract)
                .setPredicate(
                        Predicate.newBuilder().setCensusFeature(CensusFeature.TotalPopulation)
                                .setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN)
                                .setDecade(Decade._2010)
                                .setComparisonValue(2000)
                                .build()
                )
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        Iterator<TargetedCensusResponse> censusResponseIterator =
                censusBlockingStub.executeTargetedCensusQuery(request);
        while (censusResponseIterator.hasNext()) {
            TargetedCensusResponse response = censusResponseIterator.next();
            String data = response.getData();
            String responseGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
        }
    }

    private static void exampleLinearRegressionQuery(SustainGrpc.SustainBlockingStub censusBlockingStub) {
        String exampleRequest = "{\n" +
                "   \"collection\": \"future_heat\",\n" +
                "   \"feature\": \"year\",\n" +
                "   \"label\": \"temp\",\n" +
                "   \"gisJoins\": [\n" +
                "       \"G1201050\",\n" +
                "       \"G4804550\",\n" +
                "       \"G4500890\"\n" +
                "   ]\n" +
                "}";
        LinearRegressionRequest request = LinearRegressionRequest.newBuilder()
                .setRequest(exampleRequest)
                .build();

        LinearRegressionResponse response = censusBlockingStub.linearRegressionQuery(request);
        log.info(response.getResults());
    }

    public SustainGrpc.SustainBlockingStub getSustainBlockingStub() {
        return sustainBlockingStub;
    }
}

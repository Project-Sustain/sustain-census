package org.sustain.census;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.census.db.Util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SpatialClient {
    private static final Logger log = LogManager.getLogger(SpatialClient.class);

    private CensusGrpc.CensusBlockingStub censusBlockingStub;

    public SpatialClient() {
        String target = Util.getProperty(Constants.Server.HOST) + ":" + Constants.Server.PORT;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        censusBlockingStub = CensusGrpc.newBlockingStub(channel);
    }

    public CensusGrpc.CensusBlockingStub getCensusBlockingStub() {
        return censusBlockingStub;
    }

    public static void main(String[] args) {
        CensusGrpc.CensusBlockingStub censusBlockingStub = new SpatialClient().getCensusBlockingStub();

        //exampleSpatialQuery(censusBlockingStub, geoJson);
        //exampleTargetedQuery(censusBlockingStub, geoJson);
        exampleOsmQuery(censusBlockingStub, SampleGeoJson.FORT_COLLINS);
        //exampleDatasetQuery(censusBlockingStub, geoJson);
    }

    private static void exampleDatasetQuery(CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
        DatasetRequest request = DatasetRequest.newBuilder()
                .setDataset(DatasetRequest.Dataset.HOSPITALS)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();
        Iterator<DatasetResponse> datasetResponseIterator = censusBlockingStub.datasetQuery(request);
        int count = 0;
        while (datasetResponseIterator.hasNext()) {
            DatasetResponse response = datasetResponseIterator.next();
            count++;
            log.info(response.getResponse() + "\n");
        }

        log.info("Count: " + count);
    }

    private static void exampleOsmQuery(CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
        Map<String, String> requestParams = new HashMap<>();
        requestParams.put("properties.highway", "primary");
        requestParams.put("properties.highway", "residential");
        OsmRequest request = OsmRequest.newBuilder()
                .setDataset(OsmRequest.Dataset.ALL)
                .setSpatialOp(SpatialOp.GeoWithin)
                .addRequestParams(OsmRequest.OsmRequestParam.newBuilder()
                        .setKey("properties.highway")
                        .setValue("primary"))
                .addRequestParams(OsmRequest.OsmRequestParam.newBuilder()
                        .setKey("properties.highway")
                        .setValue("residential"))
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

    private static void exampleSpatialQuery(CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
        SpatialRequest request = SpatialRequest.newBuilder()
                .setCensusFeature(CensusFeature.Race)
                .setCensusResolution(CensusResolution.Tract)
                .setSpatialOp(SpatialOp.GeoWithin)
                .setRequestGeoJson(geoJson)
                .build();

        Iterator<SpatialResponse> spatialResponseIterator = censusBlockingStub.spatialQuery(request);
        while (spatialResponseIterator.hasNext()) {
            SpatialResponse response = spatialResponseIterator.next();
            String data = response.getData();
            String responseGeoJson = response.getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
        }
    }

    private static void exampleTargetedQuery(CensusGrpc.CensusBlockingStub censusBlockingStub, String geoJson) {
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
            String data = response.getSpatialResponse().getData();
            String responseGeoJson = response.getSpatialResponse().getResponseGeoJson();
            log.info("data: " + data);
            log.info("geoJson: " + responseGeoJson);
            System.out.println();
        }
    }
}

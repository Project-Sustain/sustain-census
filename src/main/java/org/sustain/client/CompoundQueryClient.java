package org.sustain.client;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.CompoundRequest;
import org.sustain.CompoundResponse;
import org.sustain.Query;
import org.sustain.SustainGrpc;
import org.sustain.db.Util;
import org.sustain.util.Constants;

import java.util.Iterator;

public class CompoundQueryClient {
    private static final Logger log = LogManager.getLogger(CompoundQueryClient.class);
    private SustainGrpc.SustainBlockingStub sustainBlockingStub;

    public CompoundQueryClient() {
        String target = Util.getProperty(Constants.Server.HOST) + ":" + 30001;
        log.info("Target: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        SustainGrpc.SustainBlockingStub sustainBlockingStub = new CompoundQueryClient().sustainBlockingStub;
        Query query1 = Query.newBuilder()
                .setHost("lattice-46")
                .setPort(27017)
                .setCollection("county_geo")
                .setQuery("{\n" +
                        "   \"geometry\": {\n" +
                        "      $geoWithin: {\n" +
                        "         $geometry: {\n" +
                        "            type: \"Polygon\" ,\n" +
                        "            coordinates:[\n" +
                        "          [\n" +
                        "            [\n" +
                        "              -105.92742919921875,\n" +
                        "              38.97222194853654\n" +
                        "            ],\n" +
                        "            [\n" +
                        "              -105.15289306640624,\n" +
                        "              38.97222194853654\n" +
                        "            ],\n" +
                        "            [\n" +
                        "              -105.15289306640624,\n" +
                        "              39.42134249546523\n" +
                        "            ],\n" +
                        "            [\n" +
                        "              -105.92742919921875,\n" +
                        "              39.42134249546523\n" +
                        "            ],\n" +
                        "            [\n" +
                        "              -105.92742919921875,\n" +
                        "              38.97222194853654\n" +
                        "            ]\n" +
                        "          ]y\n" +
                        "        ]\n" +
                        "         }\n" +
                        "      }\n" +
                        "   }\n" +
                        "}")
                .build();

        CompoundRequest request = CompoundRequest.newBuilder()
                .setFirstQuery(query1)
                .build();

        Iterator<CompoundResponse> compoundResponseIterator = sustainBlockingStub.compoundQuery(request);
        int count = 0;
        while (compoundResponseIterator.hasNext()) {
            CompoundResponse response = compoundResponseIterator.next();
            count++;
        }

        log.info("Count: " + count);
    }
}

package org.sustain.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.sustain.*;

import org.sustain.SustainGrpc;
import org.sustain.SustainGrpc.SustainBlockingStub;
import org.sustain.JsonProxyGrpc.JsonProxyBlockingStub;


/**
 * Tests gRPC calls and responses to the Sustain Server, as if a client were invoking them.
 * These tests are generally long-running, and should only be invoked on an as-need basis, instead of
 * as a prerequisite for builds. Furthermore, they are environment-specific, and will fail if the Sustain Server
 * is unable to reach the MongoDB or Spark clusters.
 */
public class SustainServerTest {

    private static final Logger log = LogManager.getLogger(SustainServerTest.class);

    private InProcessServer inProcessServer;
    private ManagedChannel channel;
    private SustainBlockingStub sustainBlockingStub;
    private JsonProxyBlockingStub jsonProxyBlockingStub;

    public SustainServerTest() {
        super();
    }

    /**
     * Example test template to test the echoQuery() RPC method.
     * Further tests should be implemented similar to this example structure.
     * @throws InterruptedException In case shutdown() is interrupted
     */
    @Tag("fast")
    @Test
    public void testExampleEchoQuery() throws InterruptedException {
        try {
            String target = "lattice-165" + ":" + 50051;
            ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            SustainBlockingStub sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
            //JsonProxyBlockingStub jsonProxyBlockingStub = JsonProxyGrpc.newBlockingStub(channel);

            InputStream ioStream = getClass().getClassLoader().getResourceAsStream(
                    "requests/linear_regression_maca_v2_request.json");
            if (ioStream != null) {
                String testingResource = new String(ioStream.readAllBytes());
                DirectRequest testRequest = DirectRequest.newBuilder()
                        .setCollection("test_collection")
                        .setQuery(testingResource)
                        .build();

                Iterator<DirectResponse> responses = sustainBlockingStub.echoQuery(testRequest);
                while (responses.hasNext()) {
                    DirectResponse response = responses.next();
                    assertEquals(response.getData(), testingResource);
                }
            }

            channel.shutdown();
        } catch (NullPointerException e) {
            log.error("NullPtr: Failed to read testing resource file: ", e.getCause());
        } catch (IOException e) {
            log.error("Failed to read testing resource file: ", e.getCause());
        } finally {
            //inProcessServer.blockUntilShutdown();

        }
    }

    /**
     * Tests the end-to-end Linear Model Request functionality.
     * Due to the long-running nature of this test, it should not be included as a unit test, but rather manually
     * invoked and verified on an as-need basis.
     * @throws InterruptedException In case shutdown() is interrupted
     */
    @Tag("slow")
    @Test
    public void testLinearRegressionModel() throws InterruptedException {
        try {
            InputStream ioStream = getClass().getClassLoader().getResourceAsStream(
                    "requests/linear_regression_maca_v2_request.json");
            if (ioStream != null) {
                String testingResource = new String(ioStream.readAllBytes());
                JsonModelRequest modelRequest = JsonModelRequest.newBuilder()
                        .setJson(testingResource)
                        .build();

                Iterator<JsonModelResponse> jsonResponseIterator = jsonProxyBlockingStub.modelQuery(modelRequest);
                while (jsonResponseIterator.hasNext()) {
                    JsonModelResponse modelResponse = jsonResponseIterator.next();
                    log.info("JSON Model Response: {}", modelResponse.getJson());
                }
            }

        } catch (NullPointerException e) {
            log.error("NullPtr: Failed to read testing resource file: ", e.getCause());
        } catch (IOException e) {
            log.error("Failed to read testing resource file: ", e.getCause());
        } finally {
            //inProcessServer.blockUntilShutdown();
            shutdown();
        }
    }

    /**
     * Tests the end-to-end K-Means Clustering Model Request functionality.
     * Due to the long-running nature of this test, it should not be included as a unit test, but rather manually
     * invoked and verified on an as-need basis.
     * @throws InterruptedException In case shutdown() is interrupted
     */
    @Tag("slow")
    @Test
    public void testKMeansClusteringModel() throws InterruptedException {
        try {
            InputStream ioStream = getClass().getClassLoader().getResourceAsStream(
                    "requests/kmeans_clustering_county_stats_request.json");
            if (ioStream != null) {
                String testingResource = new String(ioStream.readAllBytes());
                JsonModelRequest modelRequest = JsonModelRequest.newBuilder()
                        .setJson(testingResource)
                        .build();

                Iterator<JsonModelResponse> jsonModelResponseIterator = jsonProxyBlockingStub.modelQuery(modelRequest);
                while (jsonModelResponseIterator.hasNext()) {
                    JsonModelResponse jsonResponse = jsonModelResponseIterator.next();
                    log.info("JSON Model Response: {}", jsonResponse.getJson());
                }
            }

        } catch (NullPointerException e) {
            log.error("NullPtr: Failed to read testing resource file: ", e.getCause());
        } catch (IOException e) {
            log.error("Failed to read testing resource file: ", e.getCause());
        } finally {
            //inProcessServer.blockUntilShutdown();
            shutdown();
        }
    }

    @BeforeEach
    public void beforeEachTest() throws IOException {

        /*
        try {
            inProcessServer = new InProcessServer();
            inProcessServer.start();
            channel = InProcessChannelBuilder
                    .forName("test")
                    // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                    // needing certificates.
                    .usePlaintext()
                    .build();

            channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            sustainBlockingStub = SustainGrpc.newBlockingStub(channel);
            jsonProxyBlockingStub = JsonProxyGrpc.newBlockingStub(channel);
        } catch (IOException e) {
            log.error("Failed operation: " + e.getMessage());
            throw e;
        }
        */
    }

    @AfterEach
    public void afterEachTest(){
        /*
        channel.shutdownNow();
        //inProcessServer.stop();
        */
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}
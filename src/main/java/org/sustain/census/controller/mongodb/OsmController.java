package org.sustain.census.controller.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Geometry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sustain.census.Constants;
import org.sustain.census.OsmRequest;
import org.sustain.census.SpatialOp;
import org.sustain.census.db.mongodb.DBConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.sustain.census.controller.mongodb.SpatialQueryUtil.getGeometryFromGeoJson;

public class OsmController {
    private static final Logger log = LogManager.getLogger(OsmController.class);

    public static void getOsmData(OsmRequest request, OsmRequest.Dataset dataset,
                                  LinkedBlockingQueue<String> queue) {
        String datasetStr = Constants.OSM_DATASETS.get(dataset);
        Geometry geometry = getGeometryFromGeoJson(request.getRequestGeoJson());
        SpatialOp spatialOp = request.getSpatialOp();
        List<OsmRequest.OsmRequestParam> requestParamsList = request.getRequestParamsList();

        List<Bson> orFilters = new ArrayList<>();
        log.info("getOsmData({dataset: " + datasetStr + ", spatialOp: " + spatialOp + "})");

        for (OsmRequest.OsmRequestParam osmRequestParam : requestParamsList) {
            log.info("{" + osmRequestParam.getKey() + ": " + osmRequestParam.getValue() + "}");
            orFilters.add(Filters.eq(osmRequestParam.getKey(), osmRequestParam.getValue()));
        }

        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> collection = db.getCollection(datasetStr);

        Bson spatialFilter = SpatialQueryUtil.getSpatialOp(spatialOp, geometry);
        FindIterable<Document> documents;
        if (requestParamsList.size() > 0) {
            documents = collection.find(Filters.and(spatialFilter, Filters.or(orFilters)));
        } else {
            documents = collection.find(spatialFilter);
        }

        MongoCursor<Document> cursor = documents.cursor();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            Document next = cursor.next();
            queue.add(next.toJson());
        }
        log.info(datasetStr + " count: " + count);
    }
}

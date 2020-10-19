package org.sustain.proximity;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.sun.org.apache.bcel.internal.classfile.ConstantNameAndType;
import org.bson.Document;
import org.sustain.DatasetRequest;
import org.sustain.db.mongodb.DBConnection;
import org.sustain.util.Constants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

public class NearestFireStationsQuerier {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> centers = db.getCollection("tract_centers");
        MongoCollection<Document> fireStations =
                db.getCollection(Constants.DATASETS.get(DatasetRequest.Dataset.FIRE_STATIONS));
        FindIterable<Document> documents = centers.find();

        HashMap<String, HashMap<String, Double>> nearestFireStationsMap = new HashMap<>();
        int i = 0;
        // for each center
        for (Document document : documents) {
            i++;
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            double x = document.getDouble("x");
            double y = document.getDouble("y");
            String gisjoin = document.getString(Constants.GIS_JOIN);
            HashMap<String, Double> fireStationDistanceMap = new HashMap<>();

            Point refPoint = new Point(new Position(x, y));

            /**
             * db.new_stores.aggregate([
             *     { "$geoNear": {
             *         "near": {
             *             "type": "Point",
             *             "coordinates": [ -81.093699, 32.074673 ]
             *         },
             *         "maxDistance": 500 * 1609,
             *         "spherical": true,
             *         "distanceField": "distance",
             *         "distanceMultiplier": 0.000621371
             *     }}
             * ]).pretty()
             */
            double maxDistance = 1000.0;
            int fireStationsFound = 0;


            while (fireStationsFound == 0) {
                BasicDBObject dbObject = new BasicDBObject("$geoNear",
                        new BasicDBObject("near", new BasicDBObject("type", "Point")
                                .append("coordinates", new double[]{x, y}))
                                .append("maxDistance", maxDistance)
                                .append("spherical", true)
                                .append("distanceField", "distance")
                );
                AggregateIterable<Document> fireStationsNearby =
                        fireStations.aggregate(Collections.singletonList(dbObject));

                MongoCursor<Document> cursor = fireStationsNearby.cursor();
                while (cursor.hasNext()) {
                    Document next = cursor.next();
                    String fireStationId =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("id").getAsString();
                    double distance =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("distance").getAsDouble();

                    fireStationDistanceMap.put(fireStationId, distance);
                    fireStationsFound++;
                }
                maxDistance += 1000;
            }

            nearestFireStationsMap.put(gisjoin, fireStationDistanceMap);
        }

        System.out.println("Results");
        JsonArray finalArray = new JsonArray();
        for (String gisJoin : nearestFireStationsMap.keySet()) {
            JsonObject entry = new JsonObject();
            entry.addProperty(Constants.GIS_JOIN, gisJoin);

            JsonObject nearestFireStations = new JsonObject();
            HashMap<String, Double> fireStationDistanceMap = nearestFireStationsMap.get(gisJoin);
            for (String fireStationId : fireStationDistanceMap.keySet()) {
                nearestFireStations.addProperty(fireStationId, fireStationDistanceMap.get(fireStationId));
            }

            entry.add("nearest_fire_stations", nearestFireStations);
            finalArray.add(entry);
        }

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("tract_nearest_fire_stations.json")));
            writer.write(finalArray.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
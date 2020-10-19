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

public class NearestHospitalsQuerier {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> centers = db.getCollection("tract_centers");
        MongoCollection<Document> hospitals =
                db.getCollection(Constants.DATASETS.get(DatasetRequest.Dataset.HOSPITALS));
        FindIterable<Document> documents = centers.find();

        HashMap<String, HashMap<String, Double>> nearestHospitalsMap = new HashMap<>();
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
            HashMap<String, Double> hospitalDistanceMap = new HashMap<>();

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
            int hospitalsFound = 0;


            while (hospitalsFound == 0) {
                BasicDBObject dbObject = new BasicDBObject("$geoNear",
                        new BasicDBObject("near", new BasicDBObject("type", "Point")
                                .append("coordinates", new double[]{x, y}))
                                .append("maxDistance", maxDistance)
                                .append("spherical", true)
                                .append("distanceField", "distance")
                );
                AggregateIterable<Document> hospitalsNearby = hospitals.aggregate(Collections.singletonList(dbObject));

                MongoCursor<Document> cursor = hospitalsNearby.cursor();
                while (cursor.hasNext()) {
                    Document next = cursor.next();
                    String hospitalId =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("properties").getAsJsonObject().get("ID").getAsString();
                    double distance =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("distance").getAsDouble();
                    //System.out.println("Hospital ID: " + hospitalId);
                    //System.out.println("Distance: " + distance);

                    hospitalDistanceMap.put(hospitalId, distance);
                    hospitalsFound++;
                }
                maxDistance += 1000;
            }

            //System.out.println("hospitalsFound: " + hospitalsFound);
            nearestHospitalsMap.put(gisjoin, hospitalDistanceMap);
            //System.out.println();
        }

        System.out.println("Results");
        JsonArray finalArray = new JsonArray();
        for (String gisJoin : nearestHospitalsMap.keySet()) {
            JsonObject entry = new JsonObject();
            entry.addProperty(Constants.GIS_JOIN, gisJoin);

            JsonObject nearestHospitals = new JsonObject();
            HashMap<String, Double> hospitalDistanceMap = nearestHospitalsMap.get(gisJoin);
            for (String hospitalId : hospitalDistanceMap.keySet()) {
                nearestHospitals.addProperty(hospitalId, hospitalDistanceMap.get(hospitalId));
            }

            entry.add("nearest_hospitals", nearestHospitals);
            finalArray.add(entry);
        }

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("tract_nearest_hospitals.json")));
            writer.write(finalArray.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
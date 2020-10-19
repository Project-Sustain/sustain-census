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

public class NearestPowerPlantsQuerier {
    public static void main(String[] args) {
        MongoDatabase db = DBConnection.getConnection();
        MongoCollection<Document> centers = db.getCollection("tract_centers");
        MongoCollection<Document> powerPlants =
                db.getCollection(Constants.DATASETS.get(DatasetRequest.Dataset.FIRE_STATIONS));
        FindIterable<Document> documents = centers.find();

        HashMap<String, HashMap<String, Double>> nearestPowerPlantsMap = new HashMap<>();
        int i = 0;
        // for each center
        tracts:
        for (Document document : documents) {
            i++;
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            double x = document.getDouble("x");
            double y = document.getDouble("y");
            String gisjoin = document.getString(Constants.GIS_JOIN);
            HashMap<String, Double> powerPlantDistanceMap = new HashMap<>();

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
            double maxDistance = 10000.0;
            int powerPlantsFound = 0;

            while (powerPlantsFound == 0) {
                BasicDBObject dbObject = new BasicDBObject("$geoNear",
                        new BasicDBObject("near", new BasicDBObject("type", "Point")
                                .append("coordinates", new double[]{x, y}))
                                .append("maxDistance", maxDistance)
                                .append("spherical", true)
                                .append("distanceField", "distance")
                );
                AggregateIterable<Document> powerPlantsNearby =
                        powerPlants.aggregate(Collections.singletonList(dbObject));

                MongoCursor<Document> cursor = powerPlantsNearby.cursor();
                while (cursor.hasNext()) {
                    Document next = cursor.next();
                    String powerPlantId =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("id").getAsString();
                    double distance =
                            JsonParser.parseString(next.toJson()).getAsJsonObject().get("distance").getAsDouble();

                    String energySource = "";
                    try {
                        energySource = JsonParser.parseString(next.toJson()).getAsJsonObject().get("properties")
                                .getAsJsonObject().get("ENERGY_SRC").getAsString();
                    } catch (NullPointerException e) {
                        // TODO: change. should not catch NPE
                        continue;
                    }

                    if (energySource.contains("Petroleum Coke") || energySource.contains("Lignite") ||
                            energySource.contains("coal") || energySource.contains("Coal")) {
                        powerPlantDistanceMap.put(powerPlantId, distance);
                        powerPlantsFound++;
                    }
                }
                maxDistance += 10000;
                if (maxDistance > 20000) {
                    continue tracts;
                }
            }

            nearestPowerPlantsMap.put(gisjoin, powerPlantDistanceMap);
        }

        System.out.println("Results");
        JsonArray finalArray = new JsonArray();
        for (String gisJoin : nearestPowerPlantsMap.keySet()) {
            JsonObject entry = new JsonObject();
            entry.addProperty(Constants.GIS_JOIN, gisJoin);

            JsonObject nearestPowerPlants = new JsonObject();
            HashMap<String, Double> powerPlantDistanceMap = nearestPowerPlantsMap.get(gisJoin);
            for (String powerPlantId : powerPlantDistanceMap.keySet()) {
                nearestPowerPlants.addProperty(powerPlantId, powerPlantDistanceMap.get(powerPlantId));
            }

            entry.add("nearest_power_plants", nearestPowerPlants);
            finalArray.add(entry);
        }

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("tract_nearest_power_plants.json")));
            writer.write(finalArray.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
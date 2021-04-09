package org.sustain.util;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

/**
 * Created by laksheenmendis on 4/9/21 at 12:53 AM
 */
public class Helper {

    public static double calculateDistance(String metric, Vector v1, Vector v2, Integer p) throws Exception {

        double sqdist = -1d;
        switch (metric)
        {
            case "EUCLIDEAN":
                sqdist = Vectors.sqdist(v1, v2);
                break;
            case "COSINE":
                sqdist = cosineDistance(v1, v2);
                break;
            case "MINKOWSKI":
                sqdist = minkowskiDistance(v1, v2, p);
                break;
            case "UNRECOGNIZED":
                sqdist = Vectors.sqdist(v1, v2);
                break;
        }
        return sqdist;
    }

    private static double minkowskiDistance(Vector v1, Vector v2, int p) throws Exception {

        int size = v1.size();
        if(size != v2.size())
        {
            throw new Exception("Vector sizes are not equal. Cannot calculate Minkowski Distance");
        }

        double[] v1_arr = v1.toArray();
        double[] v2_arr = v2.toArray();

        double sum = 0d;
        int i=0;
        while(i < size)
        {
            sum += Math.pow(Math.abs(v1_arr[i] - v2_arr[i]) ,p);
            i++;
        }
        return Math.pow(sum, (double) 1/p);
    }

    private static double cosineDistance(Vector v1, Vector v2) {
        double dotProduct = v1.dot(v2);
        double crossProduct = Vectors.norm(v1, 2) * Vectors.norm(v2, 2);

        return dotProduct/crossProduct;
    }
}

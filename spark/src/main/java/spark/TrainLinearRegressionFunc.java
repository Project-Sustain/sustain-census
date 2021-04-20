package spark;

import org.apache.spark.api.java.function.Function;

/**
 * Function that can be serialized and passed to executors which trains the SustainModel
 * that has been unserialized into an instance.
 */
public class TrainLinearRegressionFunc implements Function<SustainLinearRegression, SustainLinearRegression> {

    @Override
    public SustainLinearRegression call(SustainLinearRegression model) throws Exception {
        model.trainModel();
        return model;
    }

}

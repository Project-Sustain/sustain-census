package spark;

import org.apache.spark.api.java.function.Function;

public class SparkMapFunctions implements Function<SerializableModel, SerializableModel> {
    @Override
    public SerializableModel call(SerializableModel v1) throws Exception {
        v1.i = v1.i * 100;
        return v1;
    }
}

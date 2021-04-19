package spark;

import org.apache.spark.api.java.function.VoidFunction;

public class SparkFunctions implements VoidFunction<SerializableModel> {

    @Override
    public void call(SerializableModel serializableModel) throws Exception {
        serializableModel.i *= 100;
    }

}

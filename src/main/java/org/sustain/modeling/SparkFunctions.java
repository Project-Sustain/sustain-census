package org.sustain.modeling;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Serializable;

public class SparkFunctions implements VoidFunction<SerializableModel> {

    public void call(SerializableModel model) {
        model.i = model.i * 100;
    }
    
}

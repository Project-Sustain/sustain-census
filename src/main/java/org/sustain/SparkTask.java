package org.sustain;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public interface SparkTask<T> {
    public abstract T execute(JavaSparkContext sparkContext, SQLContext sqlContext) throws Exception;
}

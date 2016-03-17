package com.couchbase;

import com.couchbase.spark.sql.DataFrameWriterFunctions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.collection.immutable.Map;

public class Spark {

    private SQLContext sqlContext;
    private JavaSparkContext javaSparkContext;

    public Spark(JavaSparkContext sc) {
        this.javaSparkContext = sc;
        this.sqlContext = new SQLContext(sc);
    }

    public void csvToCouchbase(String csvFilePath) {
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(csvFilePath);
        // Taking only 0.1% for test
         df = df.sample(false, 0.001);
        // Infer the schema uses the integer type for the id but we need a string
        df = df.withColumn("Id", df.col("Id").cast("string"));
        DataFrameWriterFunctions dataFrameWriterFunctions = new DataFrameWriterFunctions(df.write());
        // this option ensure the Id field will be used as key
        Map<String, String> options = new Map.Map1<String, String>("idField", "Id");
        dataFrameWriterFunctions.couchbase(options);
    }

}

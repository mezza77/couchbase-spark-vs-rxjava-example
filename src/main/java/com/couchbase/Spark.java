package com.couchbase;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import java.util.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.document.*;
import com.couchbase.spark.japi.CouchbaseSparkContext;

import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

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
        JavaRDD<JsonDocument> documentRdd = df.javaRDD().map(
            row -> {
                JsonObject object = JsonObject.create();
                String[] objectProperties = row.schema().fieldNames();
                for(int i = 0; i < objectProperties.length; i++) {
                    object.put(objectProperties[i], row.get(row.fieldIndex(objectProperties[i])));
                }
                return JsonDocument.create(object.getInt("Id").toString(), object);
            }
        );
        couchbaseDocumentRDD(documentRdd).saveToCouchbase();
    }

}

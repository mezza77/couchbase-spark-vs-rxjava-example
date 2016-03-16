package com.couchbase;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import java.util.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.spark.japi.CouchbaseSparkContext;

import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

public class SimpleApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setAppName("Simple Application")
            .setMaster("local[*]")
            .set("com.couchbase.bucket.default", "")
            .setSparkHome("/Users/nraboy/Desktop/apachespark/");
        JavaSparkContext sc = new JavaSparkContext(conf);
        CouchbaseSparkContext csc = couchbaseContext(sc);
        Spark spark = new Spark(sc);
        spark.csvToCouchbase("/Users/nraboy/Desktop/SparkProject/data/NationalNames.csv");
    }

}

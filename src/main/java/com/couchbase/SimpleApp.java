package com.couchbase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Simple Application")
                .setMaster("local[*]")
                .set("com.couchbase.bucket.audit", "audit");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Spark spark = new Spark(sc);
        spark.csvToCouchbase("/tmp/dorm.csv");
    }
}

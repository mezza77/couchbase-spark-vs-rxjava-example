package com.couchbase;

import java.io.FileReader;
import rx.Observable;
import com.opencsv.CSVReader;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class RxJava {

    private CSVReader csvReader;
    private Bucket bucket;

    public RxJava(String hostname, String bucket, String csvFilePath) {
        try {
            this.csvReader = new CSVReader(new FileReader(csvFilePath));
            this.bucket = CouchbaseCluster.create(hostname).openBucket(bucket, "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void csvToCouchbase() {
        Observable
            .from(this.csvReader)
            .map(
                csvRow -> {
                    JsonObject object = JsonObject.create();
                    object
                        .put("Name", csvRow[1])
                        .put("Year", csvRow[2])
                        .put("Gender", csvRow[3])
                        .put("Count", csvRow[4]);
                    return JsonDocument.create(csvRow[0], object);
                }
            )
            .subscribe(document -> bucket.upsert(document), error -> System.out.println(error));
    }

}

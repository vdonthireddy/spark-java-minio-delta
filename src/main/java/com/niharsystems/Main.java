package com.niharsystems;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.*;
import io.delta.tables.*;

public class Main {
    public static void main(String[] args) {
        String deltaTablePath = "vj-bucket";
        String timeNow = "abcd1234";//LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String randomString = "";//RandomStringUtils.randomAlphabetic(4);
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Simple Spark Example")
                .master("local[*]")
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                .config("spark.hadoop.fs.s3a.access.key", "2nZqqHPWEzu9JooKNoXO")
                .config("spark.hadoop.fs.s3a.secret.key", "DfFaWePTJsp5mB50pS2a7Iz00A6AgJEmdXWGyIOx")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .getOrCreate();

        //Read data from some json file
        Dataset<Row> df1 = spark.read().option("multiline", "true").json("s3a://vj-bucket/sample.json");
        df1.printSchema();
        df1.select("user.address.zip").show();

        //Create dataset
        List<Row> data = Arrays.asList(
                RowFactory.create("Vijay Donthireddy-"+randomString, 50, "Engineering-"+randomString),
                RowFactory.create("Kavitha Padera-"+randomString, 47, "Manager-"+randomString),
                RowFactory.create("Nihar Donthireddy-"+randomString, 18, "College-"+randomString),
                RowFactory.create("Nirav Donthireddy-"+randomString, 12, "Middleschool-"+randomString)
        );
        //Create schema for the above dataset
        StructType schema1 = new StructType()
                .add("name", DataTypes.StringType, false)
                .add("age", DataTypes.IntegerType, false)
                .add("department", DataTypes.StringType, false);

        //Create Dataset object from the List above
        Dataset<Row> df = spark.createDataFrame(data, schema1);

        //Write the data to minio in Delta format
        df.write()
                .format("delta")
                .mode("append")  // Can be "append" for adding new data
                .save("s3a://" + deltaTablePath + "/test-"+ timeNow);

        //Read data from minio
        Dataset<Row> dfRead = spark.read()
                .format("delta")
                .load("s3a://" + deltaTablePath + "/test-"+ timeNow);
        dfRead.show();

        DeltaTable dtPeople = DeltaTable.forPath(spark, "s3a://" + deltaTablePath + "/test-"+ timeNow);

        // Create a DataFrame with the new data
        List<Row> nd = Arrays.asList(
                RowFactory.create("Vijay Donthireddy-"+randomString, 48, "Architecture-"+randomString),
                RowFactory.create("Dummy Guy-"+randomString, 99, "SomeDepart-"+randomString)
        );
        Dataset<Row> newData = spark.createDataFrame(nd, schema1);
        // Perform the update operation
        Map<String, String> mapUpdate = new HashMap<>();
        mapUpdate.put("oldData.age", "newData.age");
        mapUpdate.put("oldData.department", "newData.department");

        Map<String, String> mapInsert = new HashMap<>();
        mapInsert.put("name", "newData.name");
        mapInsert.put("age", "newData.age");
        mapInsert.put("department", "newData.department");

        //Update the delta lake with merge
        dtPeople.as("oldData")
                .merge(
                        newData.as("newData"),
                        "oldData.name = newData.name")
                .whenMatched()
                .updateExpr(mapUpdate)
                .whenNotMatched()
                .insertExpr(mapInsert)
                .execute();

        //Read the final data
        dfRead = spark.read()
                .format("delta")
                .load("s3a://" + deltaTablePath + "/test-"+ timeNow);
        dfRead.show();

        //stop spark job
        spark.stop();
    }
}
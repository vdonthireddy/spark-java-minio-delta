package com.niharsystems;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.*;
import io.delta.tables.*;

public class Main {
    public static void main(String[] args) {
        String deltaTableBucket = "s3a://vj-bucket";
        String deltaTablePath = deltaTableBucket+"/delta-table-"+new java.text.SimpleDateFormat("yyyyMMdd-HHmm").format(new java.util.Date());;
        Boolean onlyDisplay = false;
        Boolean isInsert = !onlyDisplay && true;
        Boolean isUpdate = !onlyDisplay && !isInsert;
        Boolean isDelete = false;
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
        Dataset<Row> df1 = spark.read().option("multiline", "true").json(deltaTableBucket+"/sample.json");
        df1.printSchema();
        df1.select("user.address.zip").show();

        //Create dataset
        List<Row> data = Arrays.asList(
                RowFactory.create(1,"Vijay Donthireddy", 50, "Engineering", 80, false),
                RowFactory.create(2,"Kavitha Padera", 47, "Manager", 80, false),
                RowFactory.create(3,"Nihar Donthireddy", 18, "College", 80, false),
                RowFactory.create(4,"Nirav1 Donthireddy", 12, "Middle School", 83, false),
                RowFactory.create(5,"Nirav2 Donthireddy", 12, "High School", 90, false),
                RowFactory.create(6,"Nirav3 Donthireddy", 12, "Elementary School", 83, false),
                RowFactory.create(7,"Nirav4 Donthireddy", 12, "University of CA LA", 76, false)
        );
        //Create schema for the above dataset
        StructType schema1 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, false)
                .add("age", DataTypes.IntegerType, false)
                .add("department", DataTypes.StringType, false)
                .add("marks", DataTypes.IntegerType, false)
                .add("isdeleted", DataTypes.BooleanType, false);

        Dataset<Row> dfRead;

        if (onlyDisplay) {
            System.out.println("Vijay Display Only:");
            DeltaTable dtReadOnly = DeltaTable.forPath(spark, deltaTablePath);

            // sort by id
            dtReadOnly.df().orderBy("id").show();

            //sort by age
            dtReadOnly.df().orderBy("age").show();

            //sort by name and with case-insensitive
            dtReadOnly.df().orderBy(functions.lower(dtReadOnly.df().col("name"))).show();

            //sort by name and with case-insensitive
            dtReadOnly.df().orderBy(dtReadOnly.df().col("marks"), functions.lower(dtReadOnly.df().col("name"))).show();

            //sort by isdeleted - it's just a different way to read the data.
            spark.read()
                    .format("delta")
                    .load(deltaTablePath)
                    .orderBy("isdeleted").show();
        } else {

            if (isInsert) {
                //Create Dataset object from the List above
                Dataset<Row> df = spark.createDataFrame(data, schema1);
                System.out.println("Vijay Schema:");
                df.printSchema();

                //Write the data to minio in Delta format
                df.write()
                        .format("delta")
                        .mode("append")  // Can be "append" for adding new data
                        .partitionBy("id") //in minio if you open the delta-table folder, you can see the files organized by folders with values of 'id' column
                        .save(deltaTablePath);

                //Read data from minio
                dfRead = spark.read()
                        .format("delta")
                        .load(deltaTablePath);
                dfRead.show();
            }
            if (isUpdate) {
                //Read the final data
                dfRead = spark.read()
                        .format("delta")
                        .load(deltaTablePath);
                dfRead.orderBy("id"); // NOT working
                dfRead.show();

                DeltaTable dtPeople = DeltaTable.forPath(spark, deltaTablePath);

                // Create a DataFrame with the new data
                List<Row> nd = Arrays.asList(
                        RowFactory.create(1, "Vijay Donthireddy", 49, "Engineering", 98, true),
                        RowFactory.create(2, "Kavitha Padera", 47, "Manager", 99, true),
                        RowFactory.create(8, "new Dummy Donthireddy", 20, "UCI", 88, false),
                        RowFactory.create(3,"Nihar Donthireddy", 18, "College", 98, false),
                        RowFactory.create(4,"Nirav1 Donthireddy", 12, "Middle School", 83, false),
                        RowFactory.create(5,"Nirav2 Donthireddy", 12, "High School", 98, false),
                        RowFactory.create(6,"Nirav3 Donthireddy", 12, "Elementary School", 98, false),
                        RowFactory.create(7,"Nirav4 Donthireddy", 12, "University of CA LA", 98, false)
                );
                Dataset<Row> newData = spark.createDataFrame(nd, schema1);
                // Perform the update operation
                Map<String, String> mapUpdate = new HashMap<>();
                mapUpdate.put("oldData.id", "newData.id");
                mapUpdate.put("oldData.name", "newData.name");
                mapUpdate.put("oldData.age", "newData.age");
                mapUpdate.put("oldData.marks", "newData.marks");
                mapUpdate.put("oldData.department", "newData.department");
                mapUpdate.put("oldData.isdeleted", "newData.isdeleted");

                Map<String, String> mapInsert = new HashMap<>();
                mapInsert.put("id", "newData.id");
                mapInsert.put("name", "newData.name");
                mapInsert.put("age", "newData.age");
                mapInsert.put("marks", "newData.marks");
                mapInsert.put("department", "newData.department");
                mapInsert.put("isdeleted", "newData.isdeleted");

                //NOTE: The following commented code is NOT working. That's why it's commented. I do NOT know how to delete the data from delta table
//            //Update the delta lake with merge
//            dtPeople.as("oldData")
//                    .merge(
//                            newData.as("newData"),
//                            "oldData.id = newData.id")
//                    .whenMatched("newData.isdeleted = true")
//                    .delete()
//                    .execute();
//
//            System.out.println("Vijay After deleted:");
//            dfRead = spark.read()
//                    .format("delta")
//                    .load(deltaTablePath);
//            dfRead.orderBy("id");
//            dfRead.show();

                dtPeople.as("oldData")
                        .merge(
                                newData.as("newData"),
                                "oldData.id = newData.id")
                        .whenMatched()
                        .updateExpr(mapUpdate)
                        .whenNotMatched()
                        .insertExpr(mapInsert)
                        .execute();

                //Read the final data
                System.out.println("Vijay After updated:");
                dfRead = spark.read()
                        .format("delta")
                        .load(deltaTablePath);
                dfRead.orderBy("id");
                dfRead.show();
            }

            if (isDelete) {
                System.out.println("Vijay - Deleting rows");
                DeltaTable dtDelete = DeltaTable.forPath(spark, deltaTablePath);
                String deleteCondition = "age > 75";
                dtDelete.delete(deleteCondition);
            }
        }
        //stop spark job
        spark.stop();
    }
}
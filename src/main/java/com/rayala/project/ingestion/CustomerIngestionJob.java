package com.rayala.project.ingestion;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class CustomerIngestionJob {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Customer Ingestion Job")
                .master("local[*]")
                .getOrCreate();

        // -------------------------------
        // 1. Define schema (NO inferSchema)
        // -------------------------------
        StructType customerSchema = new StructType()
                .add("Index", DataTypes.IntegerType)
                .add("CustomerId", DataTypes.StringType)
                .add("FirstName", DataTypes.StringType)
                .add("LastName", DataTypes.StringType)
                .add("Company", DataTypes.StringType)
                .add("City", DataTypes.StringType)
                .add("Country", DataTypes.StringType)
                .add("Phone1", DataTypes.StringType)
                .add("Phone2", DataTypes.StringType)
                .add("Email", DataTypes.StringType)
                .add("SubscriptionDate", DataTypes.StringType) // read as string first
                .add("Website", DataTypes.StringType);

        // --------------------------------
        // 2. Read CSV or ZIP
        // --------------------------------
        String resourcePath="src/main/resources/cus-10000.csv";
        String fileSystemPath="file:///C:/Users/rayal/Downloads/customers-1000000.csv";
        Dataset<Row> rawDF = spark.read()
                .option("header", true)
                .option("quote", "\"")
                .option("escape", "\"")
                .option("mode", "PERMISSIVE")
                .schema(customerSchema)
//                .csv(resourcePath);
                .csv(fileSystemPath);

        // --------------------------------
        // 3. Basic validation
        // --------------------------------
        System.out.println("Total records:");
        System.out.println(rawDF.count());

        rawDF.printSchema();
        rawDF.show(5, false);

        // --------------------------------
        // 4. Convert SubscriptionDate to DATE
        // --------------------------------
        Dataset<Row> cleanDF = rawDF.withColumn(
                "SubscriptionDate",
                to_date(col("SubscriptionDate"), "yyyy-MM-dd")
        );

        // --------------------------------
        // 5. Detect bad rows
        // --------------------------------
        Dataset<Row> badRows = cleanDF.filter(col("SubscriptionDate").isNull());

        System.out.println("Bad rows (invalid date): " + badRows.count());
        badRows.show(5, false);

        // --------------------------------
        // 6. Valid rows only
        // --------------------------------
        Dataset<Row> goodRows = cleanDF.filter(col("SubscriptionDate").isNotNull());

        System.out.println("Valid rows: " + goodRows.count());

        spark.stop();
    }
}

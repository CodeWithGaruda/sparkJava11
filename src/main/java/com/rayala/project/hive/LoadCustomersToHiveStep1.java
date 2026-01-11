package com.rayala.project.hive;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class LoadCustomersToHiveStep1 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Load Customers To Hive")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        // -------------------------------
        // 1. Schema (same as ingestion)
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
                .add("SubscriptionDate", DataTypes.StringType)
                .add("Website", DataTypes.StringType);

        // -------------------------------
        // 2. Read CSV
        // -------------------------------
        Dataset<Row> rawDF = spark.read()
                .option("header", true)
                .schema(customerSchema)
                .csv("file:///C:/Users/rayal/Downloads/customers-1000000.csv");

        // -------------------------------
        // 3. Clean date
        // -------------------------------
        Dataset<Row> cleanDF = rawDF.withColumn(
                "SubscriptionDate",
                to_date(col("SubscriptionDate"), "yyyy-MM-dd")
        );

        Dataset<Row> goodDF = cleanDF.filter(col("SubscriptionDate").isNotNull());

        System.out.println("Rows to be loaded into Hive: " + goodDF.count());

        // -------------------------------
        // 4. Create database
        // -------------------------------
        spark.sql("CREATE DATABASE IF NOT EXISTS company_db");
        spark.sql("USE company_db");

        // -------------------------------
        // 5. Drop old table if exists
        // -------------------------------
        spark.sql("DROP TABLE IF EXISTS raw_customers");

        // -------------------------------
        // 6. Write to Hive (Managed Table)
        // -------------------------------
        goodDF.write()
                .mode("overwrite")
                .format("parquet")
                .saveAsTable("raw_customers");

        // -------------------------------
        // 7. Verify
        // -------------------------------
        spark.sql("SELECT COUNT(*) AS total FROM raw_customers").show();

        spark.sql("SELECT Country, COUNT(*) as count FROM raw_customers GROUP BY Country ORDER BY count DESC")
//                .show(10, false);
                .show(false);
        spark.stop();
    }
}

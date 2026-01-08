package com.rayala.dataframe.chapter3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SchemaExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ColumnOperations")
                .master("local[*]")
                .getOrCreate();

        StructType empSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, false)
                .add("department", DataTypes.StringType, true)
                .add("salary", DataTypes.IntegerType, false)
                .add("city", "string", false);

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .schema(empSchema)
//                .option("mode", "FAILFAST")
                .csv("src/main/resources/employees.csv");

        emp.printSchema();
        emp.show();

    }
}

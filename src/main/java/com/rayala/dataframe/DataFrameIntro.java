package com.rayala.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class DataFrameIntro {

    static void dfFromCollection(SparkSession spark) {

        List<Row> data = Arrays.asList(
                RowFactory.create("Amit", 23),
                RowFactory.create("Sunny", 27),
                RowFactory.create("Priya", 30)
        );

        StructType schema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.show();
        df.printSchema();

        df.filter("age > 25").show();
    }

    static void dfFromCsv(SparkSession spark) {

        StructType schema = new StructType()
                .add("id", "integer")
                .add("name", "string")
                .add("department", "string")
                .add("salary", "integer");

        //spark doesn't know its schema you have to manually give it
        Dataset<Row> emp = spark.read()
                .option("header", true)
                .schema(schema)
                .csv("src/main/resources/employees.csv");

        //or else in the options you can alse inferSchema
//        Dataset<Row> emp = spark.read()
//                .option("header", true)
//                .option("inferSchema", true)
//                .csv("src/main/resources/employees.csv");

        emp.show();
        emp.printSchema();

        // filter
        emp.filter("salary > 60000").show();

        // group by
        emp.groupBy("department")
                .avg("salary")
                .show();
    }

    static void dfFromJson(SparkSession spark) {

        Dataset<Row> people = spark.read()
                .option("multiline", true)
                .json("src/main/resources/people.json");

        people.show();
        people.printSchema();

        people.select("name", "city").show();

        people.filter("age >= 25").show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DataFrameIntro")
                .master("local[*]")
                .getOrCreate();

        // call examples
        System.out.println("----Collection---");
        dfFromCollection(spark);
        System.out.println("----CSV---");
        dfFromCsv(spark);
        System.out.println("----JSON---");
        dfFromJson(spark);

        spark.stop();
    }
}

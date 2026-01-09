package com.rayala.sparksql.chapter7;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class JavaUDF {
    static void registerSalaryBandUdf(SparkSession spark) {

        spark.udf().register(
                "salaryBand",
                (Integer salary) -> {
                    if (salary == null) {
                        return "UNKNOWN";
                    } else if (salary >= 70000) {
                        return "HIGH";
                    } else if (salary >= 50000) {
                        return "MEDIUM";
                    } else {
                        return "LOW";
                    }
                },
                DataTypes.StringType
        );
    }

    static void useUdfInSql(SparkSession spark) {
        System.out.println("---- Using UDF in SQL ----");

        spark.sql(
                "SELECT name, salary, salaryBand(salary) AS salary_band " +
                        "FROM employees"
        ).show();
    }

    static void useUdfInDataFrame(Dataset<Row> emp) {
        System.out.println("---- Using UDF in DataFrame ----");

        emp.selectExpr(
                "name",
                "salary",
                "salaryBand(salary) AS salary_band"
        ).show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlUdfExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/employees.csv");

        emp.createOrReplaceTempView("employees");

        registerSalaryBandUdf(spark);
        useUdfInSql(spark);
        useUdfInDataFrame(emp);

        spark.stop();
    }
}
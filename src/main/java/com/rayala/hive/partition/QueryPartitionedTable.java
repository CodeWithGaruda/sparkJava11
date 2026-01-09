package com.rayala.hive.partition;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class QueryPartitionedTable {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("QueryPartitionedTable")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("SHOW DATABASES").show(false);
        spark.sql("USE company_db");
        spark.sql("SHOW TABLES").show(false);

        //if not registered
//        spark.sql(
//                "CREATE TABLE IF NOT EXISTS employees_part (" +
//                        "id INT, name STRING, city STRING, salary INT, year INT" +
//                        ") PARTITIONED BY (department STRING) " +
//                        "STORED AS PARQUET " +
//                        "LOCATION 'file:///C:/spark-warehouse/company_db.db/employees_part'"
//        );
//        spark.sql("MSCK REPAIR TABLE employees_part");

        // Query only IT department (partition pruning happens here)
        Dataset<Row> result = spark.sql(
                "SELECT * FROM employees_part WHERE department = 'IT'"
        );

        result.show(false);

        // Optional: verify partition pruning
        spark.sql(
                "EXPLAIN SELECT * FROM employees_part WHERE department = 'IT'"
        ).show(false);

        spark.stop();
    }
}

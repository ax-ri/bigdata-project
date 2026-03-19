package fr.ensta.bigdata.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Utility class to convert a file from CSV to Parquet.
 */
public class CsvToParquet {
    public static void main(String[] args) {
        final String inputName = args[0];
        final String outputName = args[1];

        // Build Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("CSV To Parquet")
                .master("local")
                .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
                .getOrCreate();

        // Read the CSV file into a Data Frame
        Dataset<Row> csvDs = spark.read()
                .option("header", "true") // keep CSV header for field names
                .option("quote", "\"") // needed for proper escape handling (quotes and commas inside a value)
                .option("escape", "\"") // same as above
                .csv(inputName);

        csvDs.show(5);
        System.out.println("[info] The dataframe has " + csvDs.count() + " rows.");

        // Write the Data Frame as Parquet
        csvDs.write().mode("overwrite").parquet(outputName);

        System.out.println("[info] Successfully written Parquet File to " + outputName);

        // Reads a Parquet back into Data Frame to ensure the file is valid
        Dataset<Row> parquetDs = spark.read()
                .parquet(outputName);
        parquetDs.show(5);
        parquetDs.printSchema();

        System.out.println("[info] The Parquet dataframe has " + parquetDs.count() + " rows.");
    }
}
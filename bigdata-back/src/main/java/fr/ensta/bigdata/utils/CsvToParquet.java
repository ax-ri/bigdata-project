package fr.ensta.bigdata.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

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
                .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .getOrCreate();

        // Read the CSV file into a Data Frame
        Dataset<Row> csvDs = spark.read()
                .option("header", "true") // keep CSV header for field names
                .option("quote", "\"") // needed for proper escape handling (quotes and commas inside a value)
                .option("escape", "\"") // same as above
                .csv(inputName);

        csvDs.show(5);
        System.out.println("[info] The dataframe has " + csvDs.count() + " rows.");

        // Manually specify schema and write the Data Frame as Parquet
        csvDs.select(
                        col("id").cast("long"),
                        col("title").cast("string"),
                        col("rank").cast("long"),
                        col("date").cast("date"),
                        col("artist").cast("string"),
                        col("url").cast("string"),
                        col("region").cast("string"),
                        col("chart").cast("string"),
                        col("trend").cast("string"),
                        col("streams").cast("long"),
                        col("track_id").cast("string"),
                        col("album").cast("string"),
                        col("popularity").cast("double"),
                        col("duration_ms").cast("double"),
                        col("explicit").cast("boolean"),
                        col("release_date").cast("date"),
                        col("available_markets").cast("string"),
                        col("af_danceability").cast("double"),
                        col("af_energy").cast("double"),
                        col("af_key").cast("double"),
                        col("af_loudness").cast("double"),
                        col("af_mode").cast("boolean"),
                        col("af_speechiness").cast("double"),
                        col("af_acousticness").cast("double"),
                        col("af_instrumentalness").cast("double"),
                        col("af_liveness").cast("double"),
                        col("af_valence").cast("double"),
                        col("af_tempo").cast("double"),
                        col("af_time_signature").cast("double")
                )
                // convert null values to 0
                .na().fill(0)
                // write data in parquet format
                .write()
                .mode("overwrite")
                .parquet(outputName);

        System.out.println("[info] Successfully written Parquet File to " + outputName);

        // Reads a Parquet back into Data Frame to ensure the file is valid
        Dataset<Row> parquetDs = spark.read()
                .parquet(outputName);
        parquetDs.show(5);
        parquetDs.printSchema();

        System.out.println("[info] The Parquet dataframe has " + parquetDs.count() + " rows.");
    }
}
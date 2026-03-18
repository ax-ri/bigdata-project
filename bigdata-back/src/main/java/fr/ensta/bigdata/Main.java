package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

public final class Main {
    private static void writeDatasetAsCsv(Dataset<Row> ds, String filePath) {
        ds.coalesce(1).repartition(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Spotify Parquet")
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.sql.parquet.binaryAsString", "true")
                .getOrCreate();

        final String inputParquetPath = args[0];
        final String countryCsvPath = args[1];
        final String outputDir = args[2];

        CriteriaAggregator agg = new CriteriaAggregator(spark, inputParquetPath);
        CountryJoiner joiner = new CountryJoiner(spark, countryCsvPath);

        for (String criteria : new String[]{"af_danceability", "af_tempo", "af_speechiness"}) {
            for (int year = 2017; year <= 2021; year++) {
                Dataset<Row> ds = agg.aggregate(criteria, year);
                ds = joiner.join(ds, "region");
                writeDatasetAsCsv(ds, Path.of(outputDir, criteria + "-" + year).toString());
            }
        }
        spark.stop();
    }
}
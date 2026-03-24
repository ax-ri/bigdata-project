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

        Dataset<Row> baseDs = spark.read().parquet(inputParquetPath);
        CriteriaAggregator criteriaAgg = new CriteriaAggregator();
        SuccessAggregator successAgg = new SuccessAggregator();
        CountryJoiner joiner = new CountryJoiner(spark, countryCsvPath);

        for (int year = 2017; year <= 2021; year++) {
            for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
                // Create dataset for criteria over region (map)
                Dataset<Row> ds = criteriaAgg.aggregate(baseDs, criteria, year, 10);
                ds = joiner.join(ds, "region");
                writeDatasetAsCsv(ds, Path.of(outputDir, criteria + "-" + year).toString());
            }
        }

        for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
            // Create dataset for criteria over stream
            Dataset<Row> ds = successAgg.aggregate(baseDs, criteria);
            writeDatasetAsCsv(ds, Path.of(outputDir, "success-" + criteria).toString());
        }

        spark.stop();
    }
}
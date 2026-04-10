package fr.ensta.bigdata;

import fr.ensta.bigdata.utils.Clustering;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;

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

        spark.sparkContext().setLogLevel("WARN");

        final String inputParquetPath = args[0];
        final String countryCsvPath = args[1];
        final String outputDir = args[2];
        final int choice = Integer.parseInt(args[3]);

        Dataset<Row> baseDs = spark.read().parquet(inputParquetPath);
        CriteriaAggregator criteriaAgg = new CriteriaAggregator();
        SuccessAggregator successAgg = new SuccessAggregator();
        CountryJoiner joiner = new CountryJoiner(spark, countryCsvPath);
        Clustering clustering = new Clustering();


        // Questions:
        // - How does musical trends vary over the years?
        // - Can countries be clustered by similar music taste?
        // - Which musical characteristics (tempo, energy, etc.) give the best chance of success?

        if  (choice == 1) {
            // --> CSV file for rendering map colored by criteria (1)
            for (int year = 2017; year <= 2021; year++) {
                for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
                    Dataset<Row> ds = criteriaAgg.aggregate(baseDs, criteria, year, 10);
                    ds = joiner.join(ds, "region");
                    writeDatasetAsCsv(ds, Path.of(outputDir, criteria + "-" + year).toString());
                }
            }
        } else if (choice == 2) {
            // --> CSV file for rendering graph of criteria/number of streams (1)
            for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
                Dataset<Row> ds = successAgg.aggregate(baseDs, criteria);
                ds.show();
                writeDatasetAsCsv(ds, Path.of(outputDir, "success-" + criteria).toString());
            }
        } else if  (choice == 3) {
            // --> CSV file to cluster countries by their musical taste (2)
            Dataset<Row> clusteredDs = clustering.cluster(baseDs, 100);
            clusteredDs.show();
        } else if  (choice == 4) {
            // --> CSV file for each criterion to get the statistics of a hit (3)
            int n = 1000;
            Dataset<Row> clusteredDs = clustering.cluster(baseDs, 10000);
            Dataset<Row> ciDs = null;
            for (String criteria : new String[] {"af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_instrumentalness", "af_valence", "af_tempo", "af_time_signature"}) {
                Dataset<Row> pureDS = baseDs
                        .select("id", "streams", criteria);
                Dataset<Row> statsDs = clusteredDs
                        .join(pureDS, array_contains(clusteredDs.col("ids"), baseDs.col("id")))
                        .select("prediction", "streams", criteria)
                        .groupBy("prediction")
                        .agg(
                                functions.sum("streams").alias("streams"),
                                functions.avg(criteria).alias(criteria)
                        )
                        .orderBy(col("streams").desc())
                        .limit(n)
                        .agg(
                                functions.median(col(criteria)).alias("mean"),
                                functions.stddev(col(criteria)).alias("stddev")
                        )
                        .withColumn("criteria", lit(criteria));
                Dataset<Row> errorDs = statsDs.select(
                        col("mean"),
                        col("criteria"),
                        // confidence interval (95%)
                        expr("mean - 1.96 * (stddev / sqrt(" + n + "))").alias("ci_lower"),
                        expr("mean + 1.96 * (stddev / sqrt(" + n + "))").alias("ci_upper")
                );
                if (ciDs == null) {
                    ciDs = errorDs;
                } else {
                    ciDs = ciDs.union(errorDs);
                }
            }
            writeDatasetAsCsv(ciDs, Path.of(outputDir, "ci").toString());
        } else {
            System.out.println("[test]");
        }

        spark.stop();
    }
}
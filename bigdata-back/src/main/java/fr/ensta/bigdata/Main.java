package fr.ensta.bigdata;

import fr.ensta.bigdata.utils.Clustering;
import fr.ensta.bigdata.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;

public final class Main {

    private static String outputDir;
    private static String countryCsvPath;
    private static SparkSession spark;
    private static Dataset<Row> baseDs;

    private static void writeDatasetAsCsv(Dataset<Row> ds, String filePath) {
        ds.coalesce(1).repartition(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath);
    }

    private static SparkSession createSparkSession() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Spotify Parquet")
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.sql.parquet.binaryAsString", "true")
                .getOrCreate();
        // Remove logs from spark
        spark.sparkContext().setLogLevel("WARN");

        return spark;
    }

    private static void countryByCriteria() {
        // --> create a CSV file for rendering map colored by criteria (1)
        CriteriaAggregator criteriaAgg = new CriteriaAggregator();
        CountryJoiner joiner = new CountryJoiner(spark, countryCsvPath);

        for (int year = 2017; year <= 2021; year++) {
            for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
                Dataset<Row> ds = criteriaAgg.aggregate(baseDs, criteria, year, 10);
                ds = joiner.join(ds, "region");
                writeDatasetAsCsv(ds, Path.of(outputDir, criteria + "-" + year).toString());
            }
        }
    }

    private static void criteriaBySuccess() {
        // --> create a CSV file for rendering graph of criteria/number of streams (1)
        SuccessAggregator successAgg = new SuccessAggregator();

        for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
            Dataset<Row> ds = successAgg.aggregate(baseDs, criteria);
            writeDatasetAsCsv(ds, Path.of(outputDir, "success-" + criteria).toString());
        }
    }

    private static void countryByPrediction(int nbClusters, int songCount) {
        // --> create CSV file to cluster countries by their musical taste (2)

        Clustering clustering = new Clustering();
        Dataset<Row> clusteredSongsDs = clustering.cluster(baseDs, nbClusters);
        WindowSpec w = Window.partitionBy("region").orderBy(desc("streams"));

        for (int year = 2017; year <= 2021; year++) {
            Dataset<Row> countriesDs = Utils.getTopNSongs(baseDs, "id", songCount, year);

            Dataset<Row> clusteredCountriesDs = clusteredSongsDs
                    .join(countriesDs, array_contains(clusteredSongsDs.col("ids"), countriesDs.col("id")))
                    .select("prediction", "region", "streams")
                    .groupBy("region", "prediction")
                    .agg(
                            sum("streams").alias("streams")
                    )
                    .withColumn("rank", dense_rank().over(w))
                    .filter("rank=1")
                    .select("region", "prediction");

            writeDatasetAsCsv(clusteredCountriesDs, Path.of(outputDir, "prediction-" + year).toString());
        }
    }

    private static void hitsCharacteristics() {
        // --> create a CSV file for each criterion to get the statistics of a hit (3)
        SuccessStats stats = new SuccessStats();
        Dataset<Row> ciDs;

        for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_instrumentalness", "af_valence", "af_time_signature"}) {
            ciDs = null;
            for (int year = 2017; year <= 2021; year++) {
                Dataset<Row> statsDs = stats.aggregator(baseDs, criteria, year, 5, 1000);
                if (ciDs == null) {
                    ciDs = statsDs;
                } else {
                    ciDs = ciDs.union(statsDs);
                }
            }
            writeDatasetAsCsv(ciDs, Path.of(outputDir, "plot-" + criteria).toString());
        }
    }

    /**
     * Main function to answer those three questions:
     * - How does musical trends vary over the years?
     * - Can countries be clustered by similar music taste?
     * - Which musical characteristics (tempo, energy, etc.) give the best chance of success?
     *
     * @param args user arguments to define path to input/output files
     */
    public static void main(String[] args) {
        final String inputParquetPath = args[0];
        Main.countryCsvPath = args[1];
        Main.outputDir = args[2];

        // Create session and the base dataframe for analysis
        Main.spark = createSparkSession();
        Main.baseDs = spark.read().parquet(inputParquetPath);


        System.out.println("[INFO] begin computing streams for each criterion");
        criteriaBySuccess();
        System.out.println("[INFO] operation finished");
        System.out.println("[INFO] begin computing criterion-based value for each region");
        countryByCriteria();
        System.out.println("[INFO] operation finished");
        System.out.println("[INFO] begin computing cluster value for each region");
        countryByPrediction(100, 10);
        System.out.println("[INFO] operation finished");
        System.out.println("[INFO] begin computing cluster and criteria of the most successful hits");
        hitsCharacteristics();
        System.out.println("[INFO] operation finished");

        spark.stop();
    }
}
package fr.ensta.bigdata;

import fr.ensta.bigdata.utils.Clustering;
import fr.ensta.bigdata.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

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

    /**
     * Initialize a SparkSession with some options
     *
     * @return initialized SparkSession
     */
    private static SparkSession createSparkSession() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Spotify Parquet")
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.sql.parquet.binaryAsString", "true")
                .config("spark.sql.debug.maxToStringFields", 100)
                .getOrCreate();
        // Remove logs from spark
        spark.sparkContext().setLogLevel("WARN");

        return spark;
    }

    /**
     * Create a CSV file for rendering map colored by criteria (1)
     */
    private static void countryByCriteria() {
        CriteriaAggregator criteriaAgg = new CriteriaAggregator();
        CountryJoiner joiner = new CountryJoiner(spark, countryCsvPath);

        // Prepare base dataset
        Dataset<Row> preparedDs = baseDs
                .select(col("region"), col("streams"), col("date"), col("duration_ms"), col("af_danceability"), col("af_energy"), col("af_key"), col("af_loudness"), col("af_speechiness"), col("af_acousticness"), col("af_liveness"), col("af_valence"), col("af_tempo"))
                .cache();
        preparedDs.count();

        for (int year = 2017; year <= 2021; year++) {
            // Filtering dataset by year and caching data
            Dataset<Row> yearDS = preparedDs
                    .filter(functions.year(col("date")).equalTo(year))
                    .cache();
            yearDS.count();
            for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
                Dataset<Row> ds = criteriaAgg.aggregate(yearDS, criteria, 100);
                ds = joiner.join(ds, "region");
                writeDatasetAsCsv(ds, Path.of(outputDir, "af_map-" + criteria + "-" + year).toString());
            }
            // Cleanup
            yearDS.unpersist();
        }
        //Cleanup
        preparedDs.unpersist();
    }

    /**
     * Create CSV file to cluster countries by their musical taste (2)
     */
    private static void countryByPrediction() {
        Clustering clustering = new Clustering();
        CountryJoiner joiner = new CountryJoiner(spark, countryCsvPath);

        // Prepare clusters
        Dataset<Row> clusteredSongsDs = clustering.cluster(baseDs, 10000);
        Dataset<Row> explodedClusters = clusteredSongsDs
                .withColumn("id", functions.explode(col("ids")))
                .select(col("prediction"), col("id"))
                .cache();
        explodedClusters.count();

        // Prepare base dataset
        Dataset<Row> preparedDs = baseDs
                .select(col("id"), col("region"), col("streams"), col("date"))
                .cache();
        preparedDs.count();

        WindowSpec w = Window.partitionBy("region").orderBy(desc("streams"));

        for (int year = 2017; year <= 2021; year++) {
            Dataset<Row> countriesDs = Utils
                    .getTopNSongs(preparedDs
                            .filter(functions.year(col("date")).equalTo(year)), "id", 100);

            Dataset<Row> clusteredCountriesDs = explodedClusters
                    .join(countriesDs, "id")
                    .select(col("prediction"), col("region"), col("streams"))
                    .groupBy("region", "prediction")
                    .agg(
                            sum("streams").alias("streams")
                    )
                    .withColumn("rank", functions.row_number().over(w))
                    .filter(col("rank").equalTo(1))
                    .select("region", "prediction");
            clusteredCountriesDs = joiner.join(clusteredCountriesDs, "region");

            writeDatasetAsCsv(clusteredCountriesDs, Path.of(outputDir, "cluster_map-" + year).toString());
        }
        // Cleanup
        preparedDs.unpersist();
        explodedClusters.unpersist();
    }

    /**
     * Create a CSV file for rendering graph of criteria/number of streams (1)
     */
    private static void criteriaBySuccess() {
        SuccessAggregator successAgg = new SuccessAggregator();

        // Prepare base dataset
        Dataset<Row> preparedDs = baseDs
                .groupBy("title")
                .agg(
                        sum("streams").alias("streams"),
                        functions.avg("duration_ms").alias("duration_ms"),
                        functions.avg("af_danceability").alias("af_danceability"),
                        functions.avg("af_energy").alias("af_energy"),
                        functions.avg("af_key").alias("af_key"),
                        functions.avg("af_loudness").alias("af_loudness"),
                        functions.avg("af_speechiness").alias("af_speechiness"),
                        functions.avg("af_acousticness").alias("af_acousticness"),
                        functions.avg("af_liveness").alias("af_liveness"),
                        functions.avg("af_valence").alias("af_valence"),
                        functions.avg("af_tempo").alias("af_tempo")
                )
                .cache();
        preparedDs.count();

        for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_liveness", "af_valence", "af_tempo"}) {
            Dataset<Row> ds = successAgg.aggregate(preparedDs, criteria);
            writeDatasetAsCsv(ds, Path.of(outputDir, "success_plot-" + criteria).toString());
        }
        //Cleanup
        preparedDs.unpersist();
    }

    /**
     * Create a CSV file for each criterion to get the statistics of a hit (3)
     */
    private static void hitsCharacteristics() {
        SuccessStats stats = new SuccessStats();
        Clustering cluster = new Clustering();

        // Prepare base dataset
        Dataset<Row> ciDs;
        Dataset<Row> preparedDs = baseDs
                .select("id", "streams", "date",
                        "duration_ms", "af_danceability", "af_energy",
                        "af_key", "af_loudness", "af_speechiness",
                        "af_acousticness", "af_instrumentalness",
                        "af_valence", "af_time_signature"
                )
                .withColumn("year", functions.year(col("date")))
                .cache();

        preparedDs.count();

        // Prepare clusters
        Dataset<Row> clusteredDs = cluster.cluster(baseDs, 5);
        Dataset<Row> explodedClusters = clusteredDs
                .withColumn("id", functions.explode(col("ids")))
                .select(col("prediction"), col("id"))
                .cache();
        explodedClusters.count();

        // Prepare join dataset
        Dataset<Row> joinDs = explodedClusters
                .join(preparedDs, "id")
                .select("prediction", "streams", "year",
                        "duration_ms", "af_danceability", "af_energy",
                        "af_key", "af_loudness", "af_speechiness",
                        "af_acousticness", "af_instrumentalness",
                        "af_valence", "af_time_signature"
                )
                .cache();
        joinDs.count();

        // Take min(1000, min(number of songs per clusters))
        int songCount = 1000;
        long songsByCluster = clusteredDs.
                groupBy("prediction")
                .count()
                .select(min("count"))
                .first()
                .getLong(0);
        if (songCount > songsByCluster) {
            songCount = (int) songsByCluster - 1;
        }

        for (String criteria : new String[]{"duration_ms", "af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_instrumentalness", "af_valence", "af_time_signature"}) {
            ciDs = null;
            Dataset<Row> criteriaDs = joinDs
                    .select("prediction", "streams", "year", criteria)
                    .cache();

            criteriaDs.count();

            for (int year = 2017; year <= 2021; year++) {
                Dataset<Row> statsDs = stats.aggregator(criteriaDs, criteria, year, songCount);
                if (ciDs == null) {
                    ciDs = statsDs;
                } else {
                    ciDs = ciDs.union(statsDs);
                }
            }
            writeDatasetAsCsv(ciDs, Path.of(outputDir, "af_plot-" + criteria).toString());
            // Cleanup
            criteriaDs.unpersist();
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

        System.out.println("[INFO] begin computing criteria-based value for each region");
        countryByCriteria();
        System.out.println("[INFO] operation finished");
        System.out.println("[INFO] begin computing cluster value for each region");
        countryByPrediction();
        System.out.println("[INFO] operation finished");
        System.out.println("[INFO] begin computing streams/criteria for each song");
        criteriaBySuccess();
        System.out.println("[INFO] operation finished");
        System.out.println("[INFO] begin computing cluster and criteria of the most successful hits");
        hitsCharacteristics();
        System.out.println("[INFO] operation finished");

        spark.stop();
    }
}
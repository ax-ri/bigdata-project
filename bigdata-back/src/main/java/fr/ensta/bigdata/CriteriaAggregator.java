package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * Create aggregated data for a given criteria, for further analysis.
 */
public class CriteriaAggregator {
    private final SparkSession spark;
    private final String inputParquetPath;

    public CriteriaAggregator(SparkSession spark, String inputParquetPath) {
        this.spark = spark;
        this.inputParquetPath = inputParquetPath;
    }

    /**
     * Load dataset from disk with the correct schema.
     *
     * @return dataset
     */
    private Dataset<Row> readDataset() {
        // get the dataset from the spark session
        Dataset<Row> ds = this.spark.read()
                .format("parquet")
                .option("inferSchema", "true")
                .load(this.inputParquetPath);

        // cast the column to the right type (schema)
        return ds.select(
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
                .na().fill(0);
    }

    /**
     * Normalize the values of the column named `metric` such that the minimum is at 0 and the maximum is at 1.
     *
     * @param ds source dataset
     * @return modified dataset
     */
    private Dataset<Row> normalizeValue(Dataset<Row> ds) {
        final double max = ds.agg(max("metric")).alias("max").first().getDouble(0);
        final double min = ds.agg(min("metric")).alias("min").first().getDouble(0);
        final double n = max == min ? 1 : max - min;
        return ds.withColumn("metric", col("metric").minus(min).divide(n));
    }

    /**
     * Calculate the most popular 'genre' in each region (for the given year).
     *
     * @param criteria  criteria from which the 'genre' is defined
     * @param year      given year
     * @param songCount number of most popular song to consider
     * @return final dataset
     */
    public Dataset<Row> aggregate(String criteria, int year, int songCount) {
        WindowSpec w = Window.partitionBy("region").orderBy(desc("streams"));
        return normalizeValue(
                readDataset()
                        .filter("YEAR(date) = '" + year + "'")
                        .select("region", criteria, "streams")
                        .withColumn("rank", dense_rank().over(w))
                        .filter(col("rank").$less$eq(songCount))
                        .groupBy("region").avg(criteria)
                        .withColumnRenamed("avg(" + criteria + ")", "metric")
        );
    }
}

package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        return ds.na().fill(0).select(
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
                col("popularity").cast("float"),
                col("duration_ms").cast("double"),
                col("explicit").cast("boolean"),
                col("release_date").cast("date"),
                col("available_markets").cast("string"),
                col("af_danceability").cast("float"),
                col("af_energy").cast("float"),
                col("af_key").cast("float"),
                col("af_loudness").cast("float"),
                col("af_mode").cast("boolean"),
                col("af_speechiness").cast("float"),
                col("af_acousticness").cast("float"),
                col("af_instrumentalness").cast("float"),
                col("af_liveness").cast("float"),
                col("af_valence").cast("float"),
                col("af_tempo").cast("float"),
                col("af_time_signature").cast("float")
        );
    }

    /**
     * Normalize the values of a given criteria such that the minimum is at 0 and the maximum is at 1.
     *
     * @param ds       source dataset
     * @param criteria column to normalize
     * @return modified dataset
     */
    private Dataset<Row> normalizeValue(Dataset<Row> ds, String criteria) {
        final float max = ds.agg(max((ds.col(criteria)))).alias("max").first().getFloat(0);
        final float min = ds.agg(min((ds.col(criteria)))).alias("min").first().getFloat(0);
        final float n = max == min ? 1 : max - min;
        return ds.withColumn(criteria, col(criteria).minus(min).divide(n));
    }

    /**
     * Calculate the most popular 'genre' in each region (for the given year).
     *
     * @param criteria criteria from which the 'genre' is defined
     * @param year     given year
     * @return final dataset
     */
    public Dataset<Row> aggregate(String criteria, Integer year) {
        Dataset<Row> dsCast = readDataset();
        Dataset<Row> dsYear = dsCast.filter("YEAR(date) = '" + year + "'");
        Dataset<Row> dsYearRestricted = dsYear.select("region", criteria, "streams");
        Dataset<Row> dsPop = dsYearRestricted.groupBy("region").max("streams").withColumnRenamed("max(streams)", "streams");
        Dataset<Row> dsEnd = dsYearRestricted.join(dsPop, new String[]{"region", "streams"}, "left_semi");
        return normalizeValue(dsEnd.select("region", criteria), criteria).withColumnRenamed(criteria, "metric");
    }
}

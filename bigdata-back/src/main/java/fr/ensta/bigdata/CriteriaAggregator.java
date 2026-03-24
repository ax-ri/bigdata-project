package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * Create aggregated data for a given criteria, for further analysis.
 */
public class CriteriaAggregator {
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
    public Dataset<Row> aggregate(Dataset<Row> baseDs, String criteria, int year, int songCount) {
        WindowSpec w = Window.partitionBy("region").orderBy(desc("streams"));
        return normalizeValue(
                baseDs
                        .filter("YEAR(date) = '" + year + "'")
                        .cache()
                        .select("region", criteria, "streams")
                        .withColumn("rank", dense_rank().over(w))
                        .filter(col("rank").$less$eq(songCount))
                        .groupBy("region").avg(criteria)
                        .withColumnRenamed("avg(" + criteria + ")", "metric")
        );
    }
}

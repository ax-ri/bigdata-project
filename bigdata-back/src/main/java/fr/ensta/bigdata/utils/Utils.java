package fr.ensta.bigdata.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class Utils {

    /**
     * Normalize the values of the column whose name  is `criteria` such that the minimum is at 0 and the maximum is at 1.
     *
     * @param ds       source dataset
     * @param criteria column name
     * @return modified dataset
     */
    public static Dataset<Row> normalizeValue(Dataset<Row> ds, String criteria) {
        final double max = ds.agg(max(criteria)).alias("max").first().getDouble(0);
        final double min = ds.agg(min(criteria)).alias("min").first().getDouble(0);
        final double n = max == min ? 1 : max - min;
        return ds.withColumn(criteria, col(criteria).minus(min).divide(n));
    }

    public static Dataset<Row> getTopNSongs(Dataset<Row> baseDs, String column, int songCount, int year) {
        WindowSpec w = Window.partitionBy("region").orderBy(desc("streams"));

        return baseDs
                .filter("YEAR(date) = '" + year + "'")
                .cache()
                .select(column, "region", "streams")
                .withColumn("rank", dense_rank().over(w))
                .filter(col("rank").$less$eq(songCount));
    }
}

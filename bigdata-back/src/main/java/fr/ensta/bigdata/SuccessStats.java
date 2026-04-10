package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class SuccessStats {
    public Dataset<Row> aggregator(Dataset<Row> joinDs, String criteria, int year, int songCount) {
        WindowSpec w = Window
                .partitionBy(criteria)
                .orderBy(col("streams").desc());


        Dataset<Row> statsDs = joinDs
                .select("prediction", "streams", "year", criteria)
                .filter(col("year").equalTo(year))
                .groupBy("prediction")
                .agg(
                        functions.sum("streams").alias("streams"),
                        functions.avg(criteria).alias(criteria)
                )
                .withColumn("rank", row_number().over(w))
                .filter(col("rank").leq(songCount))
                .agg(
                        functions.median(col(criteria)).alias("mean"),
                        functions.stddev(col(criteria)).alias("stddev")
                )
                .withColumn("year", lit(year));
        Dataset<Row> errorDs = statsDs.select(
                col("mean"),
                col("year"),
                // confidence interval (95%)
                expr("mean - 1.96 * (stddev / sqrt(" + songCount + "))").alias("ci_lower"),
                expr("mean + 1.96 * (stddev / sqrt(" + songCount + "))").alias("ci_upper")
        );
        return errorDs;
    }
}

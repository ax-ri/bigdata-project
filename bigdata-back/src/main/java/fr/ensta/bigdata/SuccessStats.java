package fr.ensta.bigdata;

import fr.ensta.bigdata.utils.Clustering;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class SuccessStats {

    Clustering clustering = new Clustering();


    public Dataset<Row> aggregator(Dataset<Row> baseDs, String criteria, int year, int nbClusters, int songCount) {
        Dataset<Row> clusteredDs = clustering.cluster(baseDs, nbClusters);

        // Assert songCount is smaller than the number of songs in each clusters
        long songsByCluster = clusteredDs.groupBy("prediction").count().select(min("count")).first().getLong(0);
        if (songCount > songsByCluster) {
            songCount = (int) songsByCluster - 1;
        }

        Dataset<Row> pureDS = baseDs
                .select("id", "streams", "date", criteria);
        Dataset<Row> statsDs = clusteredDs
                .join(pureDS, array_contains(clusteredDs.col("ids"), pureDS.col("id")))
                .select("prediction", "streams", "date", criteria)
                .filter("YEAR(date)=" + year)
                .groupBy("prediction")
                .agg(
                        functions.sum("streams").alias("streams"),
                        functions.avg(criteria).alias(criteria)
                )
                .orderBy(col("streams").desc())
                .limit(songCount)
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

package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class SuccessAggregator {
    /**
     * Calculate, for each track, its total of streams and its average rank on 'criteria'
     *
     * @param ds       base dataset
     * @param criteria criteria on which the average rank is calculated
     * @return dataset containing streams vs average criteria
     */
    public Dataset<Row> aggregate(Dataset<Row> ds, String criteria) {
        return ds
                .select(col("streams"), col(criteria))
                .groupBy(criteria)
                .agg(
                        functions.sum("streams").alias("streams")
                )
                .orderBy(criteria);
    }
}
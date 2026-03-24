package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

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
        Map<String, String> hash = new HashMap<>();
        hash.put("streams", "sum");
        hash.put(criteria, "avg");
        return ds
                .cache()
                .select("title", "streams", criteria)
                .groupBy("title").agg(hash)
                .withColumnRenamed("sum(streams)", "total")
                .withColumnRenamed("avg(" + criteria + ")", "metrics")
                .select("total", "metrics")
                .filter(col("total").notEqual(0))
                .orderBy("total");
    }
}

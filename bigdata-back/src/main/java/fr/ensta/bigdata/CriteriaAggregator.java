package fr.ensta.bigdata;

import fr.ensta.bigdata.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Create aggregated data for a given criteria, for further analysis.
 */
public class CriteriaAggregator {
    /**
     * Calculate the most popular 'genre' in each region (for the given year).
     *
     * @param criteria  criteria from which the 'genre' is defined
     * @param songCount number of most popular song to consider
     * @return final dataset
     */
    public Dataset<Row> aggregate(Dataset<Row> baseDs, String criteria, int songCount) {
        return Utils.normalizeValue(
                Utils.getTopNSongs(baseDs, criteria, songCount)
                        .groupBy("region")
                        .avg(criteria)
                        .withColumnRenamed("avg(" + criteria + ")", "criteria"),
                "criteria"
        );
    }
}

package fr.ensta.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.Normalizer;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 * Helper class to add a country code column to a dataset with country names.
 */
public class CountryJoiner {
    private final SparkSession spark;
    private final String countryCsvPath;

    public CountryJoiner(SparkSession spark, String countryCsvPath) {
        this.spark = spark;
        this.countryCsvPath = countryCsvPath;

        this.spark.udf().register(
                "normalize",
                (String s) -> {
                    if (s == null) return null;
                    return normalized(s);
                },
                org.apache.spark.sql.types.DataTypes.StringType
        );
    }

    /**
     * Remove spaces and special characters (accents etc.) from a string.
     *
     * @param s string to normalize
     * @return normalized string
     */
    private static String normalized(String s) {
        return Normalizer
                .normalize(s, Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "")
                .replaceAll(" ", "")
                .toLowerCase();
    }

    /**
     * Join a dataset containing a column with country names with the dataset containing country codes.
     *
     * @param ds               final dataset
     * @param countryFieldName country column name in the given dataframe
     * @return dataset with the country column replaced by a column with country codes
     */
    public Dataset<Row> join(Dataset<Row> ds, String countryFieldName) {
        Dataset<Row> countryInfo = this.spark.read()
                .option("header", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .csv(this.countryCsvPath)
                .drop("iso2", "iso_num", "country")
                .withColumn("country_normalized", callUDF("normalize", col("country_common")));
        return ds
                .withColumn("country_normalized", callUDF("normalize", col(countryFieldName)))
                .join(countryInfo, new String[]{"country_normalized"})
                .drop("country_common", "country_normalized", countryFieldName)
                .withColumnRenamed("iso3", "country");
    }
}

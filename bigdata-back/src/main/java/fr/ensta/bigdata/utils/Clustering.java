package fr.ensta.bigdata.utils;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;


public final class Clustering {
    /**
     * Prepare the dataset to be clustered (regroup by title, normalize values, create vector)
     *
     * @param dataset source dataset
     * @return modified dataset
     */
    private Dataset<Row> prepareDataset(Dataset<Row> dataset) {
        // Regroup identical music (title/artist)
        Dataset<Row> ds = dataset
                .groupBy("title")
                .agg(
                        functions.collect_list("id").alias("ids"),
                        functions.avg("af_danceability").alias("af_danceability"),
                        functions.avg("af_energy").alias("af_energy"),
                        functions.avg("af_key").alias("af_key"),
                        functions.avg("af_loudness").alias("af_loudness"),
                        functions.avg("af_speechiness").alias("af_speechiness"),
                        functions.avg("af_acousticness").alias("af_acousticness"),
                        functions.avg("af_instrumentalness").alias("af_instrumentalness"),
                        functions.avg("af_valence").alias("af_valence"),
                        functions.avg("af_tempo").alias("af_tempo"),
                        functions.avg("af_time_signature").alias("af_time_signature"));

        // Normalize each audio feature
        for (String criteria : new String[]{"af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_instrumentalness", "af_valence", "af_tempo", "af_time_signature"}) {
            dataset = Utils.normalizeValue(ds, criteria);
            ds = dataset;
        }

        // Select the cluster vector
        VectorAssembler vec = new VectorAssembler("clustering")
                .setInputCols(new String[]{"af_danceability", "af_energy", "af_key", "af_loudness", "af_speechiness", "af_acousticness", "af_instrumentalness", "af_valence", "af_tempo", "af_time_signature"})
                .setOutputCol("features");

        return vec
                .transform(ds)
                .select("ids", "title", "features");
    }

    /**
     * Cluster music by their audio features
     *
     * @param dataset    source dataset
     * @param nbClusters number of clusters
     * @return clustered dataset [ids | prediction ]
     */
    public Dataset<Row> cluster(Dataset<Row> dataset, int nbClusters) {
        // Prepares dataset
        Dataset<Row> clusteredDs = prepareDataset(dataset);

        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(nbClusters).setSeed(1L);
        KMeansModel model = kmeans.fit(clusteredDs);

        // Make predictions
        Dataset<Row> predictions = model.transform(clusteredDs);

        // Evaluate clustering by computing Silhouette score (need to be positif)
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(predictions);
        assert silhouette > 0;

        return predictions.select("ids", "prediction");
    }
}
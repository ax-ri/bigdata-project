# Reflection questions 

> **1. What distributed computing principle(s) did your project rely on? Explain how your workload is split and parallelized across a cluster (e.g., partitioning strategy, shuffle operations).**

ANSWER 

> **2. How did you choose your storage format and why? Discuss trade-offs between formats you considered (e.g., CSV vs Parquet vs Avro) in terms of schema evolution, compression, and read performance.**

During this project, we considered two storage formats: `CSV` (original) and `Parquet` (final). We chose to work with the `Parquet` format for the main operations because of:
- its **column-based storage**: to answer our question, we had a lot of column-operators (as `group by` or `average`) that are faster using `parquet`;
- its **compression** techniques: at the end, the `parquet` file has a size divided by ~20; 
- its preservation of **data types**: our dataset contains columns with double, boolean or date data that can be used directly with their types during operations (e.g. when searching the `max` of a column, it has to be a `int` or `double`); 
- we didn't have interest into writing or updating the original `parquet` file during our project (`parquet` is slower when updating rows).

However, we used the `CSV` format for our final results: the analysis outputs a `CSV` file with the computed metrics, to be used by visualization. We used `CSV` for this part because it was enough for our needs: the final data is very small (a number for each country, i.e. ~200 rows) and thus we did not need the complexity of another format.

> **3. What performance optimization(s) did you apply, or would you apply with more time?**
> 
> Examples: partitioning, caching, broadcast joins, predicate pushdown, choice of API (RDD vs DataFrame), cluster sizing.

Our optimizations are mainly based on **caching** existing `Dataset`. Each time we reuse a dataset, in nested loop for example, we tested if caching the dataset reduced cost and if so, we use it in our pipeline. 

We try to use some other optimizations based on **filtering by statistics before using windows** (e.g., filtering by quartile), but the computation was slower with this optimization. We think it was because the data was not big enough to ensure that computing quartile and filtering was coster than computing the window without it. The same problem occurred with **broadcast joins**, and we didn't use it either.

All optimizations were done using the `explain` function and the execution time.

With more time, we would have liked to learn more deeply how to use partitioning and clustering in our pipeline in a smarter way. 
We also discovered the idea of `stack()` transformation to only have one distributed computation instead of nested loops (that creates many small computation). We didn't have time to replace all nested loops with only one pipeline, but it would likely have optimized our computation.

> **4. If your dataset were 100x larger, what would break in your current pipeline, and how would you fix it? Think about bottlenecks, data skew, memory limits, and infrastructure changes.**

If it were 100x larger, it would likely break because of:
- our usage of `cache()` that could cause a disk spilling if the dataset is too large\
**Fix**: only cache small dataset or use `MEMORY_AND_DISK` on [storage level]("https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html")
- `clustering` which is already one of our most costly function (need to iterate multiple times over the dataset) and is not well optimized because of the need to latter explode the `ids` column\
**Fix**: we could try to explode clusters immediately and cache it for latter use
- functions as `groupBy`, `orderBy`, `window` (for now, the window function only apply to an extremely small part of the dataset) or `joins` that could spill to disk and become extremely slow because of `Spark` shuffling (need to shuffle all data across the cluster) and partitions sorting\
**Fix**: need to pre-aggregate data earlier in the pipeline and use a better partitioning to avoid multiple shuffles
- a possible unbalanced dataset: if one region have a larger number of song than the other, one executor will have to handle more than the other and becomes a limitation\
**Fix**: using salt when using `groupBy` function on region


> **5. What was the most valuable thing you learned in this course, and how did it influence your project?**

ANSWER
# Reflection questions 

> **1. What distributed computing principle(s) did your project rely on? Explain how your workload is split and parallelized across a cluster (e.g., partitioning strategy, shuffle operations).**

ANSWER 

> **2. How did you choose your storage format and why? Discuss trade-offs between formats you considered (e.g., CSV vs Parquet vs Avro) in terms of schema evolution, compression, and read performance.**

During this project, we considered two storage formats: `CSV` (original) and `Parquet` (final). We chose to work with the `Parquet` format for the main operations because of:
- its **column-based storage**: to answer our question, we had a lot of column-operators (as `group by` or `average`) that are faster using `parquet`;
- its **compression** techniques: at the end, the `parquet` file has a size divided by ~20; 
- its preservation of **data types**: our dataset contains columns with double, boolean or date data that can be used directly with their types during operations (e. g. when searching the `max` of a column, it has to be a `int` or `double`); 
- we didn't have interest into writing or updating the original `parquet` file during our project (`parquet` is slower when updating rows).

The following table illustrates the advantages of `parquet` format over `CSV`:

|     criteria     | `CSV` | `Parquet` |
| :--------------: | :---: | :-------: |
|       size       |  27G  |   1.2G    |
| read performance | #TODO |   #TODO   |


However, we used the `CSV` format for our final results: the analysis outputs a `CSV` file with the computed metrics, to be used by visualization. We used `CSV` for this part because it was enough for our needs: the final data is very small (a number for each country, i.e. ~200 rows) and thus we did not need the complexity of another format.

> **3. What performance optimization(s) did you apply or would you apply with more time?**
> 
> Examples: partitioning, caching, broadcast joins, predicate pushdown, choice of API (RDD vs DataFrame), cluster sizing.

ANSWER:
- caching (better usage possible)
- filtering (quartiles) before using windows (#TODO)
- look at predicate pushdown (what does it do exactly/stat ?)
- use DataFrame instead of RDD
- clustering/partitioning (?)

> **4. If your dataset were 100x larger, what would break in your current pipeline and how would you fix it? Think about bottlenecks, data skew, memory limits, and infrastructure changes.**

ANSWER

> **5. What was the most valuable thing you learned in this course, and how did it influence your project?**

ANSWER
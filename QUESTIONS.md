# Reflection questions 

> **What distributed computing principle(s) did your project rely on? Explain how your workload is split and parallelized across a cluster (e.g., partitioning strategy, shuffle operations).**

ANSWER 

> **How did you choose your storage format and why? Discuss trade-offs between formats you considered (e.g., CSV vs Parquet vs Avro) in terms of schema evolution, compression, and read performance.**

During this project, we considered two storage formats: `CSV` (original) and `Parquet` (final). We chose to work with `Parquet` format for the main operations because of:
- its **column-based storage**: to answer our question, we had a lot of column-operators (as `group by` or `average`) that are faster using `parquet`;
- its **compression** techniques: at the end, the `parquet` file has a size divided by ~20; 
- its preservation of **data types**: our dataset contains columns with double, boolean or date data that can be used directly with their types during operations (eg. when searching the `max` of a column, it has to be a `int` or `double`); 
- we did'nt have interest into write or update the original `parquet` file during our project (`parquet` is slower when updating rows).

The following table illustrates the advantages of `parquet` format over `CSV`:

|     criteria     | `CSV` | `Parquet` |
| :--------------: | :---: | :-------: |
|       size       |  27G  |   1.2G    |
| read performance | #TODO |   #TODO   |


However, **--> EXPLAIN WHY WE CHOSE TO USE CSV FOR RENDERING MAPS <--**

> **What performance optimization(s) did you apply or would you apply with more time?**
> 
> Examples: partitioning, caching, broadcast joins, predicate pushdown, choice of API (RDD vs DataFrame), cluster sizing.

ANSWER:
- caching (better usage possible)
- filtering (quartiles) before using windows (#TODO)
- look at predicate pushdown (what does it do exactly/stat ?)
- use DataFrame instead of RDD
- clustering/partitioning (?)

> **If your dataset were 100x larger, what would break in your current pipeline and how would you fix it? Think about bottlenecks, data skew, memory limits, and infrastructure changes.**

ANSWER

> **What was the most valuable thing you learned in this course, and how did it influence your project?**

ANSWER
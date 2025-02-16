# cuDF Benchmarks

This project aims to benchmark data processing and explore the implementation of the [cuDF library](https://github.com/rapidsai/cudf) from the [RAPIDS](https://rapids.ai/) team. The primary objective is to accelerate data pipelines using GPUs through [NVIDIA CUDA](https://developer.nvidia.com/cuda-toolkit) and [Apache Arrow](https://arrow.apache.org/). In this benchmark, the performance of the libraries `pandas` (with and without cuDF), `polars` (with and without cuDF), and `PySpark` will be compared.
This work was inspired by [TeoMeWhy's](https://github.com/TeoMeWhy/rapids-benchmark) benchmark.

### Setup
- Ubuntu 24.04.2 LTS
- AMD Ryzen 7 7800x3D @ 5.05 GHz
- NVIDIA GeForce RTX 4080 SUPER 16GB
- 32GB RAM @ 6000MHz

### Environment
- Python 3.12.3
- NVIDIA driver 550.144.03
- CUDA Version: 12.4
- RAPIDS 24.12

### Data

For this benchmark, the [Goodreads Book Reviews](https://www.kaggle.com/datasets/pypiahmad/goodreads-book-reviews1) dataset from Kaggle was used. It is a simple dataframe with five columns, containing information about the relationship between users and the books they have read or want to read. The dataset contains over 225 million rows and more than 800,000 unique users.  
<img src="./images/Pasted image 20250216190335.png">  

Two complete dataframes were used for this benchmark: one in `.csv` format (4.3GB) and another in `.parquet` format (1.1GB). Additionally, multiple smaller dataframes were created, containing only the `user_id` and `book_id` columns, with row counts ranging from 500,000 to 200 million.

### Methodology

For each benchmark, the same process was repeated 12 times. The highest and lowest execution times were discarded, and the average execution time was calculated. The resulting `.csv` files with the benchmark results are available in this repository.

### Benchmark 1 - Complete Database

The first benchmark involved reading the entire Goodreads dataframe, grouping the data by `user_id`, counting the `book_id` values, and calculating the average `rating`.  
<img src="./images/Pasted image 20250216191630.png">  

For this case, the table below illustrates the data

| engine       | file    | mean     | stddev   | median   | Rate   |
|:------------:|:-------:|---------:|---------:|---------:|-------:|
| spark        | parquet | 0.038362 | 0.007237 | 0.035753 | 1.00   |
| spark        | csv     | 0.061850 | 0.011694 | 0.065556 | 1.61   |
| polars_cudf  | parquet | 0.340801 | 0.100387 | 0.308305 | 8.88   |
| polars_cudf  | csv     | 1.074315 | 0.140156 | 1.030304 | 28.00  |
| polars       | parquet | 3.133524 | 0.027382 | 3.120671 | 81.68  |
| pandas_cudf  | parquet | 3.135356 | 0.248288 | 3.052279 | 81.73  |
| polars       | csv     | 3.801188 | 0.023988 | 3.793273 | 99.09  |
| pandas       | parquet | 5.234514 | 0.246346 | 5.141955 | 136.45 |
| pandas_cudf  | csv     | 8.046553 | 1.118021 | 7.713310 | 209.75 |
| pandas       | csv     | 28.676856| 0.241594 | 28.730991| 747.53 |

### Benchmark 2 - Multiple dataframes

In this benchmark, multiple dataframes with row counts ranging from 500,000 to 200 million were tested. The process involved reading the file, grouping the data by `user_id`, and counting the `book_id` values.  
<img src="./images/Pasted image 20250216191923.png">  
As expected, all cuDF implementations were faster than their original counterparts, with the exception of `pandas` when reading `.csv` files. During this execution, between 90 million and 95 million rows, the VRAM limit appeared to be reached. After three separate executions, the same error was consistently reported:  
`[RMM] [error] [A][Stream 0x1][Upstream 800000000B][FAILURE maximum pool size exceeded]`  
At the time of writing, a solution to this issue has not been identified.

It is evident that for smaller datasets (<10 million rows), the differences in execution times are negligible and may not be noticeable to the user (unless reading `.csv` files with `pandas`).  
<img src="./images/Pasted image 20250216192634.png">  

However, beyond this threshold, the performance gap between cuDF implementations (and PySpark) begins to increase almost linearly. In some cases, execution times were up to 13 times faster (e.g., `polars` with `.parquet` files).

### Conclusion

This benchmark was a simple test to explore the implementation of cuDF and measure its performance. Although more time was spent troubleshooting CUDA and RAPIDS versioning issues than coding, the experience was valuable, as it provided an opportunity to learn something new.

### Next Steps

- Experiment with different and more complex operations.
- Learn how to use and benchmark with [DuckDB](https://duckdb.org/).
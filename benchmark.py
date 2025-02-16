import argparse
import time
import pandas as pd
import polars as pl
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = (
    SparkSession.builder 
    .appName("cudf-benchmark")   
    .master("local[*]")        
    .config("spark.driver.memory", "22g")   
    .config("spark.executor.memory", "22g") 
    .getOrCreate()
)

def read_parquet(n, engine):
    if (engine == "pandas"):
        start = time.time()
        df = pd.read_parquet(f"./data/parquet/users_{n}.parquet")
        df = df.groupby("user_id").agg({"book_id": "count"})
        res = time.time() - start
        del df
        return res
    elif (engine == "pandas_cudf"):
        start = time.time()
        df = pd.read_parquet(f"./data/parquet/users_{n}.parquet")
        df = df.groupby("user_id").agg({"book_id": "count"})
        res = time.time() - start
        del df
        return res
    elif (engine == "polars"):
        start = time.time()
        df = pl.scan_parquet(f"./data/parquet/users_{n}.parquet")
        df = df.group_by("user_id").agg(pl.col("book_id").len()).collect()
        res = time.time() - start
        del df
        return res
    elif (engine == "polars_cudf"):
        start = time.time()
        df = pl.scan_parquet(f"./data/parquet/users_{n}.parquet")
        df = df.group_by("user_id").agg(pl.col("book_id").len()).collect(engine="gpu")
        res = time.time() - start
        del df
        return res
    elif (engine == "spark"):
        start = time.time()
        df = spark.read.parquet(f"./data/parquet/users_{n}.parquet")
        df = df.groupBy("user_id").agg(f.count("book_id"))
        res = time.time() - start
        del df
        return res


def read_csv(n, engine):
    if (engine == "pandas"):
        start = time.time()
        df = pd.read_csv(f"./data/csv/users_{n}.csv")
        df = df.groupby("user_id").agg({"book_id": "count"})
        res = time.time() - start
        del df
        return res
    elif (engine == "pandas_cudf"):
        start = time.time()
        df = pd.read_csv(f"./data/csv/users_{n}.csv")
        df = df.groupby("user_id").agg({"book_id": "count"})
        res = time.time() - start
        del df
        return res
    elif (engine == "polars"):
        start = time.time()
        df = pl.scan_csv(f"./data/csv/users_{n}.csv")
        df = df.group_by("user_id").agg(pl.col("book_id").len()).collect()
        res = time.time() - start
        del df
        return res
    elif (engine == "polars_cudf"):
        start = time.time()
        df = pl.scan_csv(f"./data/csv/users_{n}.csv")
        df = df.group_by("user_id").agg(pl.col("book_id").len()).collect(engine="gpu")
        res = time.time() - start
        del df
        return res
    elif (engine == "spark"):
        start = time.time()
        df = spark.read.csv(f"./data/csv/users_{n}.csv", header=True)
        df = df.groupBy("user_id").agg(f.count("book_id"))
        res = time.time() - start
        del df
        return res

def main():
    today = datetime.today().strftime("%Y%m%d")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--engine", 
        choices=["pandas", "pandas_cudf", "polars", "polars_cudf", "spark"], 
        default="pandas", 
        required=True,
        help="Choose processing engine: pandas (CPU), pandas_cudf (GPU), polars (CPU), polars_cudf (GPU) or spark (CPU)"
    )
    parser.add_argument(
        "--size", 
        default=500000, 
        required=True, 
        type=int,
        help="Number of rows in the dataset"
    )
    parser.add_argument(
        "--file_format", 
        choices=["parquet", "csv"], 
        default="parquet", 
        required=True,
        help="Input file format: parquet or csv"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of times to repeat the operation (default: 1)"
    )
    args = parser.parse_args()


    execs = {
        "csv": read_csv,
        "parquet": read_parquet
    }

    results = []
    for i in range(args.iterations):
        try:
            res = execs[args.file_format](args.size, args.engine)
            results.append(res)
            
            with open(f"benchmark_results_{today}.csv", "a") as f:
                f.write(f"{args.engine},{args.file_format},{args.size},{i+1},{res}\n")
                
        except Exception as e:
            print(f"Error in iteration {i+1}: {str(e)}")
            continue

    if args.iterations > 1:
        avg_time = sum(results) / len(results)
        print(f"\nSummary Statistics using {args.engine} reading {args.file_format} files with {args.size} lines:")
        print(f"Average execution time: {avg_time:.4f} seconds")
        print(f"Best time: {min(results):.4f} seconds")
        print(f"Worst time: {max(results):.4f} seconds")
            
if __name__ == "__main__":
    main()
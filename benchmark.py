import argparse
import time
import pandas as pd


def pandas_read_csv(n):
    start = time.time()
    df = pd.read_csv(f"./data/csv/users_{n}.csv")
    df.groupby("user_id").agg({"book_id": "count"})
    res = time.time() - start
    return res

def pandas_read_parquet():
    start = time.time()
    df = pd.read_parquet("/home/mello/projects/rapids-cudf/data/interim/imdb_db/")
    df.groupby("titleType").agg({"averageRating": "mean", "tconst": "count"})
    res = time.time() - start
    return res

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--engine", 
        choices=["pandas", "cudf"], 
        default="pandas", 
        required=True,
        help="Choose processing engine: pandas (CPU) or cudf (GPU)"
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
        "csv": {
            "pandas": pandas_read_csv,
            "cudf": pandas_read_csv
        },
        "parquet": {
            "pandas": pandas_read_parquet,
            "cudf": pandas_read_parquet
        }
    }

    results = []
    for i in range(args.iterations):
        try:
            res = execs[args.file_format][args.engine](args.size)
            results.append(res)
            
            with open("benchmark_results.csv", "a") as f:
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
            
    # Write results to CSV
    with open("benchmark_results.csv", "a") as f:
        f.write(f"{args.engine},{args.file_format},{res}\n")
                

if __name__ == "__main__":
    main()
source .venv/bin/activate;
rm benchmark_results.csv;
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 500000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 1000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 5000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 10000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 15000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 20000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 25000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 30000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 35000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 40000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 45000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 50000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 55000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 60000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 65000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 70000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 75000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 80000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 85000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 90000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 95000000
python benchmark.py --engine pandas --file_format csv --iterations 12 --size 100000000

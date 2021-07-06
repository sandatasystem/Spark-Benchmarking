#spark-benchmark

## What does it do
This tool will perform benchmarks using teragen, terasort, teravalidate and a join script

## How To Run
Specify the type of benchmarks to run

The following will run the join and teragen, terasort and teravalidate benchmarks:

```./benchmark.sh namespace_name --join 10 100 1000 --tera 10 100 1000```

Please refer to the runbook for more information on how to properly run the benchmarks


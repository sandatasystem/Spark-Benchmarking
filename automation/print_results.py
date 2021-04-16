from tabulate import tabulate
import sys

table = []
if sys.argv[1] == "join":
    benchmarks = sys.argv[2].split("\n")
    number_of_rows = sys.argv[3].split(" ")

    for benchmark, row in zip(benchmarks, number_of_rows):
       table.append(["Inner Join", row, row, benchmark])

elif sys.argv[1] == "terrasort":
    print(sys.argv)
    benchmarks = sys.argv[2].split("\n")

    table.append(["Sorting", benchmarks[0]])
    table.append(["Aggregation", benchmarks[1]])
    table.append(["Partial Aggregation", benchmarks[2]])

    print(table)
    print("\n\n\n {} \n\n\n".format(tabulate(table, headers=["Operation", "Benchmark Timing"])))


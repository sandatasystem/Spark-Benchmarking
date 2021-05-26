#!/bin/bash
namespace=$1
shift

teragen_rows=()
join_rows=()
while [[ $# -gt 0 ]]
do

    argument="$1"

    case $argument in
	--tera|-teragen-benchmark)
	shift

        teragen_rows+=($1)
        teragen_rows+=($2)
        teragen_rows+=($3)

	shift
	shift
        shift

	;;
        --join)
        shift

        join_rows+=($1)
        join_rows+=($2)
        join_rows+=($3)


	shift
	shift
        shift
	;;
    esac
done

echo "teragen rows is ${teragen_rows[*]}"
echo "join rows is ${join_rows[*]}"

$HOME/Spark-Benchmarking/automation/setup_phase.sh $namespace
$HOME/Spark-Benchmarking/automation/submit_spark_jobs_phase.sh $namespace "${teragen_rows[*]}" "${join_rows[*]}"
$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace

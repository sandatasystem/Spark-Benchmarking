#!/bin/bash
namespace=$1
shift

teragen_rows=()
join_rows=()

tera=false
join=false

while [[ $# -gt 0 ]]
do

    argument="$1"

    case $argument in
	--tera|-teragen-benchmark)
	shift

        tera=true
        join=false

	;;
        --join)
        shift

        join=true
        tera=false

	;;
        *)
        if [[ "$argument" =~ ^[0-9]+$ ]]
        then
           if [[ "$tera" = true ]]
           then
               teragen_rows+=($argument)
               shift
           elif [[ "$join" = true ]]
           then
               join_rows+=($argument)
               shift
           fi
        fi
    esac
done


$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace
$HOME/Spark-Benchmarking/automation/setup_phase.sh $namespace

if [[ "$?" -eq "1" ]]
then
    exit 1
fi

$HOME/Spark-Benchmarking/automation/submit_spark_jobs_phase.sh $namespace "${teragen_rows[*]}" "${join_rows[*]}"
$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace

printf "Benchmarks complete .."

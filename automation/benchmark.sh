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

printf "\n\n\n\n\nSTARTING SETUP PHASE \n\n\n\n\n"
$HOME/Spark-Benchmarking/automation/setup_phase.sh $namespace

if [[ "$?" -eq "1" ]]
then
    exit 1
fi

printf "\n\n\n\n\nSUBMITTING JOBS FOR BENCHMARKING \n\n\n\n\n"
$HOME/Spark-Benchmarking/automation/submit_spark_jobs_phase.sh $namespace "${teragen_rows[*]}" "${join_rows[*]}"
printf "\n\n\n\n\nSUBMITTING CLEANUP PHASE \n\n\n\n\n"
#$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace

printf "Benchmarks complete .."

namespace=$1

$HOME/Spark-Benchmarking/automation/setup_phase.sh $namespace
$HOME/Spark-Benchmarking/automation/submit_spark_jobs_phase.sh $namespace
$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace

namespace=$1

# Submitting join script command
printf "\n\n\n\nSUBMITTING JOIN SCRIPT\n\n\n\n"
export DRIVER_NUMBER_OF_CORES=1
export EXECUTOR_NUMBER_OF_CORES=1
export NUMBER_OF_EXECUTOR_INSTANCES=3
# Join script program will generate 20K, 40K, 60K row dataframes
export DF_ROWS_ARGUMENTS="20000 40000 60000"
export NUMBER_OF_ARGUMENTS=$(echo $DF_ROWS_ARGUMENTS | wc -w)

# Submitting join script
j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-join.yaml > /tmp/spark-benchmark-join.yaml
kubectl apply -f /tmp/spark-benchmark-join.yaml -n $namespace
sleep 150
benchmarks=$(kubectl logs spark-benchmark-join-driver -n t01 | grep -Po -m $NUMBER_OF_ARGUMENTS "(\d*\.?\d*) seconds$")
printf "\n\n\nFINISHED JOIN SCRIPT. BENCHMARK TIMES ARE \n\n"
python $HOME/Spark-Benchmarking/automation/print_results.py join "$benchmarks" "$DF_ROWS_ARGUMENTS"
kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-join -n t01

# Submitting terragen script
printf "\n\n\n\nSUBMITTING TERRAGEN SCRIPT\n\n\n\n"
export NUMBER_OF_ROWS_TO_GENERATE=200000
j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-terragen.yaml > /tmp/spark-benchmark-terragen.yaml
kubectl apply -f /tmp/spark-benchmark-terragen.yaml -n $namespace
sleep 110
kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-terragen -n t01
printf "\n\n\n\nFINISHED TERRAGEN SCRIPT \n\n\n\n"

# Submitting terrasort script
printf "\n\n\n\nSUBMITTING TERRASORT SCRIPT\n\n\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-terrasort.yaml -n $namespace
sleep 150
benchmarks=$(kubectl logs spark-benchmark-terrasort-driver -n t01 | grep -Po -m 3 "(\d*\.?\d*) seconds$")
printf "\n\n\nFINISHED TERRASORT. BENCHMARK TIMES ARE \n\n"
python $HOME/Spark-Benchmarking/automation/print_results.py terrasort "$benchmarks"
kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-terrasort -n t01

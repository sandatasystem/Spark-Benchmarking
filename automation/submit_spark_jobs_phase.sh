namespace=$1
read -r -a teragen_rows <<< "$2"
read -r -a join_rows <<< "$3"

function poll_till_benchmark_completes(){
    spark_operator_name=$1
    max_wait_time=$2

    printf "\n\n\n\nWAITING UNTIL BENCHMARK HAS FINISHED FOR OPERATOR: $spark_operator_name \n\n\n\n"
    while true; do
	benchmark=$(kubectl logs $spark_operator_name -n $namespace | grep -Po "(\d*\.?\d*) seconds$")

        # If the length of the benchmark is greater than 0, we know the job has finished
	if [[ "${#benchmark}" -gt "0" ]]
	then
	    return 0
	fi

	sleep 1
	max_wait_time=$((max_wait_time-1))

        # If the wait time has hit 0, we know the job never finished and there is likely an error
	if [[ "$max_wait_time" -eq "0" ]]
	then
            printf "\n\n\n\nTHERE MAYBE AN ERROR IN THE $spark_operator_name JOB. LOGS: \n\n\n\n\n"
            printf `kubectl logs $spark_operator_name -n $namespace`
	    exit 1
	fi
    done

}

function wait_till_sparkoperator_pod_deleted(){
    # This method will wait till the driver pod for the spark operator is deleted
    spark_operator_pod=$1
    while true; do
        check_if_operator_deleted=$(kubectl get pods -n $namespace | grep -o $spark_operator_pod)

        if [[ "${#check_if_operator_deleted}" -eq "0" ]]
        then
            printf "\n\n\n\nSPARK OPERATOR DRIVER POD $spark_operator_pod HAS BEEN DELETED\n\n\n\n"
            return 0
        fi
    done   
}

function submit_join_job(){
    # Join script program will generate 500, 1000, 2000 row dataframes and then join them
    printf "\n\n\n\nSUBMITTING JOIN SCRIPT\n\n\n\n"
    export DRIVER_NUMBER_OF_CORES=1
    export EXECUTOR_NUMBER_OF_CORES=1
    export NUMBER_OF_EXECUTOR_INSTANCES=3

    for row_size in "${join_rows[@]}"
    do
	for join_type in inner left right
	do
	    export DF_ROWS_ARGUMENTS="$row_size"
	    export JOIN_TYPE="$join_type"

	    j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-join.yaml > /tmp/spark-benchmark-join.yaml
	    kubectl apply -f /tmp/spark-benchmark-join.yaml -n $namespace

            # wait till pod and container gets created 
            printf "\n\n\n\nWAITING FOR POD AND CONTAINER TO GET CREATED\n\n\n\n"
            sleep 120
            poll_till_benchmark_completes "spark-benchmark-join-driver" 500

	    benchmark=$(kubectl logs spark-benchmark-join-driver -n $namespace | grep -Po "(\d*\.?\d*) seconds$")
	    printf "\n\n\n\nFINISHED JOIN SCRIPT. TIME TO $join_type JOIN 2 DATAFRAME OF SIZE $row_size is $benchmark\n\n\n\n"
	    kubectl delete sparkapplication spark-benchmark-join -n $namespace

            wait_till_sparkoperator_pod_deleted spark-benchmark-join-driver 
	done
    done
}

function submit_teragen_terasort_teravalidate(){
    export DRIVER_NUMBER_OF_CORES=1
    export EXECUTOR_NUMBER_OF_CORES=1
    export NUMBER_OF_EXECUTOR_INSTANCES=3

    export TERAGEN_OUTPUT_DIR="/spark-benchmark-mount/teragen-files"

    export TERASORT_INPUT_DIR="/spark-benchmark-mount/teragen-files"
    export TERASORT_OUTPUT_DIR="/spark-benchmark-mount/teravalidate-files"

    export TERAVALIDATE_INPUT_DIR="/spark-benchmark-mount/teravalidate-files"

    for row_size in "${teragen_rows[@]}"
    do
        # TERAGEN
	export DF_ROWS_ARGUMENTS="$row_size"
        printf "PERFORMING TERAGEN ON $row_size ROWS"
	j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-teragen.yaml > /tmp/spark-benchmark-teragen.yaml
	kubectl apply -f /tmp/spark-benchmark-teragen.yaml -n $namespace

        sleep 120
        poll_till_benchmark_completes "spark-benchmark-teragen-driver" 1000
	benchmark=$(kubectl logs spark-benchmark-teragen-driver -n $namespace | grep -Po "(\d*\.?\d*) seconds$")
	printf "\n\n\n FINISHED TERAGEN SRIPT. TIME TO GENERATE $row_size rows is $benchmark seconds"
	kubectl delete sparkapplication spark-benchmark-teragen -n $namespace
        wait_till_sparkoperator_pod_deleted spark-benchmark-teragen-driver

        # TERASORT
        printf "PERFORMING TERASORT ON $row_size ROWS"
	j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-terasort.yaml > /tmp/spark-benchmark-terasort.yaml
	kubectl apply -f /tmp/spark-benchmark-terasort.yaml -n $namespace 

        sleep 50
        poll_till_benchmark_completes "spark-benchmark-terasort-driver" 1000
	benchmark=$(kubectl logs spark-benchmark-terasort-driver -n $namespace | grep -Po "(\d*\.?\d*) seconds$")
	printf "FINISHED TERASORT SRIPT. TIME TO SORT $row_size rows is $benchmark seconds"
        kubectl delete sparkapplication spark-benchmark-terasort -n $namespace
        wait_till_sparkoperator_pod_deleted spark-benchmark-terasort-driver

        # TERAVALIDATE
        printf "PERFORMING TERAVALIDATE ON $NUMBER_OF_ROWS_TO_GENERATE ROWS"
	j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-teravalidate.yaml > /tmp/spark-benchmark-teravalidate.yaml
	kubectl apply -f /tmp/spark-benchmark-teravalidate.yaml -n $namespace

	sleep 50
        poll_till_benchmark_completes "spark-benchmark-teragen-driver" 1000
	benchmark=$(kubectl logs spark-benchmark-teravalidate-driver -n $namespace | grep -Po "(\d*\.?\d*) seconds$")
	printf "FINISHED TERAVALIDATE SRIPT. TIME TO VALIDATE $row_size rows is $benchmark seconds"
	kubectl delete sparkapplication spark-benchmark-teravalidate -n $namespace
        wait_till_sparkoperator_pod_deleted spark-benchmark-teravalidate-driver

        sleep 50
    done
}

if [[ "${#join_rows[@]}" -gt "0" ]]; then
    submit_join_job
fi

if [[ "${#teragen_rows[@]}" -gt "0" ]]; then
    submit_teragen_terasort_teravalidate
fi

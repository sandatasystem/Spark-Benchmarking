namespace=$1

printf "\n\n\n\nSUBMITTING JOIN SCRIPT\n\n\n\n"
export DRIVER_NUMBER_OF_CORES=1
export EXECUTOR_NUMBER_OF_CORES=1
export NUMBER_OF_EXECUTOR_INSTANCES=3

# Join script program will generate 20K, 40K, 60K row dataframes
function submit_join_job(){
    for row_size in 500 1000 2000
    do
	for join_type in inner left right
	do
	    export DF_ROWS_ARGUMENTS="$row_size"
	    export JOIN_TYPE="$join_type"

	    echo "DF ROW ARGUMENTS IS $DF_ROWS_ARGUMENTS and JOIN TYPE IS $JOIN_TYPE"
	    j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-join.yaml > /tmp/spark-benchmark-join.yaml
	    kubectl apply -f /tmp/spark-benchmark-join.yaml -n $namespace

	    if [ $row_size == 500 ]
	    then
		sleep 120
	    elif [ $row_size == 1000 ]
	    then
		sleep 150
	    elif [ $row_size == 2000 ]
	    then
		sleep 250
	    fi

	    benchmark=$(kubectl logs spark-benchmark-join-driver -n t01 | grep -Po "(\d*\.?\d*) seconds$")
	    printf "\n\n\nFINISHED JOIN SCRIPT. TIME TO $join_type JOIN 2 DATAFRAME OF SIZE $row_size is $benchmark\n\n"
	    kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-join -n t01
	done
    done
}

function tera_sleep(){
    if [ "$1" == "2000" ]
    then
	sleep 100
    elif [ "$1" == "10000" ]
    then
	sleep 140
    elif [ "$1" == "100000" ]
    then
	sleep 180
    fi
}

function submit_teragen_terasort_teravalidate(){
    # Perform teragen, terasort and teravalidate on 2000 rows,  10000 rows, 100000 rows
    export TERAGEN_OUTPUT_DIR="/spark-benchmark-mount/teragen-files"

    export TERASORT_INPUT_DIR="/spark-benchmark-mount/teragen-files"
    export TERASORT_OUTPUT_DIR="/spark-benchmark-mount/teravalidate-files"

    export TERAVALIDATE_INPUT_DIR="/spark-benchmark-mount/teravalidate-files"

    for row_size in 2000 10000 100000
    do
        # TERAGEN
	export NUMBER_OF_ROWS_TO_GENERATE="$row_size"
        printf "PERFORMING TERAGEN ON $NUMBER_OF_ROWS_TO_GENERATE ROWS"
	j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-teragen.yaml > /tmp/spark-benchmark-teragen.yaml
	kubectl apply -f /tmp/spark-benchmark-teragen.yaml -n $namespace
        tera_sleep $row_size
	benchmark=$(kubectl logs spark-benchmark-teragen-driver -n t01 | grep -Po "(\d*\.?\d*) seconds$")
	printf "\n\n\n FINISHED TERAGEN SRIPT. TIME TO GENERATE $row_size rows is $benchmark seconds"
	kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-teragen -n t01

        # TERASORT
        printf "PERFORMING TERASORT ON $NUMBER_OF_ROWS_TO_GENERATE ROWS"
	j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-terasort.yaml > /tmp/spark-benchmark-terasort.yaml
	kubectl apply -f /tmp/spark-benchmark-terasort.yaml -n $namespace 
	tera_sleep $row_size
	benchmark=$(kubectl logs spark-benchmark-terasort-driver -n t01 | grep -Po "(\d*\.?\d*) seconds$")
	printf "FINISHED TERASORT SRIPT. TIME TO SORT $row_size rows is $benchmark seconds"
        kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-terasort -n t01

        # TERAVALIDATE
        printf "PERFORMING TERAVALIDATE ON $NUMBER_OF_ROWS_TO_GENERATE ROWS"
	j2 $HOME/Spark-Benchmarking/yamls/spark-benchmark-teravalidate.yaml > /tmp/spark-benchmark-teravalidate.yaml
	kubectl apply -f /tmp/spark-benchmark-teravalidate.yaml -n $namespace 
	tera_sleep $row_size
	benchmark=$(kubectl logs spark-benchmark-teravalidate-driver -n t01 | grep -Po "(\d*\.?\d*) seconds$")
	printf "FINISHED TERAVALIDATE SRIPT. TIME TO VALIDAE $row_size rows is $benchmark seconds"
	kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-teravalidate -n t01
    done
}

# submit_join_job
submit_teragen_terasort_teravalidate


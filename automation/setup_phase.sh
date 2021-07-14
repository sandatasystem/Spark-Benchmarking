namespace=$1

function wait_till_pvc_created(){
    pvc_name=$1

    printf "IF CREATION OF PVC TAKES TOO LONG OR FAILS USE kubectl get pvc AND kubectl describe pvc TO DEBUG"

    while true; do
        status=$(kubectl get pvc -n $namespace $pvc_name -o jsonpath="{.status.phase}")
        if [[ "$status" == "Bound" ]]
	then
	    return 0
        elif [[ "$status" -ne "Pending" ]]
        then
            printf "SOMETHING WENT WRONG WHEN TRYING TO CREATE PVC: $pvc_name. USE THE ABOVE COMMANDS TO FIGURE OUT WHAT WENT WRONG"
        fi
    done
}

function wait_till_pod_running(){
    pod_name=$1

    printf "IF CREATION OF POD TAKES TOO LONG OR FAILS USE kubectl get pod AND kubectl describe pod TO DEBUG"
    
    while true; do
        status=$(kubectl get pod -n $namespace $pod_name -o jsonpath="{.status.phase}")
        if [[ "$status" == "Running" ]]
	then
	    return 0
        elif [[ "$status" -ne "Pending" ]]
        then
            printf "SOMETHING WENT WRONG WHEN TRYING TO CREATE POD: $pod_name. USE THE ABOVE COMMANDS TO FIGURE OUT WHAT WENT WRONG"
	fi
    done
}

if [[ -z "${USERNAME}" ]] || [[ -z "${PASSWORD}" ]]
then
    printf "\n\nLDAP USERNAME OR PASSWORD NOT SET. SET 'USERNAME' AND 'PASSWORD' ENVIRONMENT VARIABLES\n\n"
    exit 1
fi

# Perform cleanup if last run was cancelled before cleanup phase
printf "\n\nRUNNING INITIAL CLEANUP \n\n"
$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace

# Mapr ticket generation
printf "\n\nPERFORMING MAPRTICKET GENERATION FOR USER: $USERNAME \n\n"
kubectl exec -it tenantcli-0 bash -n $namespace -- bash -c "printf '${USERNAME}\n${PASSWORD}\njenkins-secret\nn' | kubernetes/ticketcreator.sh">/dev/null
sleep 10

# Creating a PVC
printf "\n\nCREATING PVC\n\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-pvc.yaml -n $namespace
wait_till_pvc_created spark-benchmark-claim

# Creating a temp pod
printf "\n\nCREATING TEMP POD\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-temp-pod.yaml -n $namespace
wait_till_pod_running spark-benchmark-temp-pod

# Copying all python scripts to PVC
printf "\n\nCOPYING SCRIPTS TO PVC\n\n"
kubectl cp $HOME/Spark-Benchmarking/scripts $namespace/spark-benchmark-temp-pod:/spark-benchmark-mount/ &

# Create teragen-files directory and make sure it has permissions for spark teragen script to write to 
printf "\n\nCREATING TERRAGEN DIRECTORY\n\n"
kubectl exec -n $namespace -it spark-benchmark-temp-pod -- mkdir -p spark-benchmark-mount/teragen-files
kubectl exec -n $namespace -it spark-benchmark-temp-pod -- chmod -R 777 spark-benchmark-mount/teragen-files

namespace=$1


# Perform cleanup if last run was cancelled before cleanup phase
printf "\n\n\n\n RUNNING INITIAL CLEANUP \n\n\n\n"
$HOME/Spark-Benchmarking/automation/cleanup_phase.sh $namespace

# Mapr ticket generation
printf "\n\n\n\n PERFORMING MAPRTICKET GENERATION FOR USER: $USERNAME \n\n\n\n"
kubectl exec -it tenantcli-0 bash -n $namespace -- bash -c "printf '${USERNAME}\n${PASSWORD}\njenkins-secret\nn' | kubernetes/ticketcreator.sh">/dev/null
sleep 10

# Creating a PVC
printf "\n\n\n\nCREATING PVC\n\n\n\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-pvc.yaml -n $namespace
sleep 15

# Creating a temp pod
printf "\n\n\n\nCREATING TEMP POD\n\n\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-temp-pod.yaml -n $namespace
sleep 40

# Copying all python scripts to PVC
printf "\n\n\n\nCOPYING SCRIPTS TO PVC\n\n\n\n"
kubectl cp $HOME/Spark-Benchmarking/scripts $namespace/spark-benchmark-temp-pod:/spark-benchmark-mount/ &
sleep 10

# Create teragen-files directory and make sure it has permissions for spark teragen script to write to 
printf "\n\n\n\nCREATING TERRAGEN DIRECTORY\n\n\n\n"
kubectl exec -n $namespace -it spark-benchmark-temp-pod -- mkdir -p spark-benchmark-mount/teragen-files
kubectl exec -n $namespace -it spark-benchmark-temp-pod -- chmod -R 777 spark-benchmark-mount/teragen-files
sleep 10

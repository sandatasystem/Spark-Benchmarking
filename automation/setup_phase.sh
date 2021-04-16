namespace=$1

# Mapr ticket generation
export USERNAME="alice"
export PASSWORD="balice"

printf "\n\n\n\n PERFORMING MAPRTICKET GENERATION FOR USER: $USERNAME \n\n\n\n"
kubectl exec -it tenantcli-0 bash -n t01 -- bash -c "printf '$USERNAME\n$PASSWORD\njenkins-secret\nn' | kubernetes/ticketcreator.sh">/dev/null
sleep 5

# Creating a PVC
printf "\n\n\n\nCREATING PVC\n\n\n\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-pvc.yaml -n $namespace
sleep 10

# Creating a temp pod
printf "\n\n\n\nCREATING TEMP POD\n\n\n\n"
kubectl apply -f $HOME/Spark-Benchmarking/yamls/spark-benchmark-temp-pod.yaml -n $namespace
sleep 35

# Copying all python scripts to PVC
printf "\n\n\n\nCOPYING SCRIPTS TO PVC\n\n\n\n"
kubectl cp $HOME/Spark-Benchmarking/scripts t01/spark-benchmark-temp-pod:/spark-benchmark-mount/ &
sleep 10

# Create terragen-files directory and make sure it has permissions for spark terragen script to write to 
printf "\n\n\n\nCREATING TERRAGEN DIRECTORY\n\n\n\n"
kubectl exec -n t01 -it spark-benchmark-temp-pod -- mkdir -p spark-benchmark-mount/terragen-files
#TODO: Provide correct permissions on this directory instead of using 777
kubectl exec -n t01 -it spark-benchmark-temp-pod -- chmod -R 777 spark-benchmark-mount/terragen-files
sleep 10

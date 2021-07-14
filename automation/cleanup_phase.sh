namespace=$1

printf "\n\n\n\n\nSUBMITTING CLEANUP PHASE \n\n\n\n\n"

printf "\n\n\tDELETING SPARK JOIN OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-join -n $namespace --ignore-not-found=true>/dev/null

printf "\n\n\tDELETING SPARK TERAGEN OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-teragen -n $namespace --ignore-not-found=true>/dev/null

printf "\n\n\tDELETING SPARK TERSORT OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-terasort -n $namespace --ignore-not-found=true>/dev/null

printf "\n\n\tDELETING SPARK TERAVALIDATE OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-teravalidate -n $namespace --ignore-not-found=true>/dev/null

printf "\n\n\tDELETING TEMP POD\n\n"
kubectl delete pod spark-benchmark-temp-pod -n $namespace --ignore-not-found=true>/dev/null

printf "\n\n\tDELETING PVC\n\n"
kubectl delete pvc spark-benchmark-claim -n $namespace --ignore-not-found=true>/dev/null

printf "\n\n\tDELETING MAPR TICKET\n\n"
kubectl delete secret jenkins-secret -n $namespace --ignore-not-found=true>/dev/null

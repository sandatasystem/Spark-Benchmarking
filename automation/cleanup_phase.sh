namespace=$1

printf "\n\nDELETING SPARK JOIN OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-join -n $namespace --ignore-not-found=true

printf "\n\nDELETING SPARK TERAGEN OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-teragen -n $namespace --ignore-not-found=true

printf "\n\nDELETING SPARK TERSORT OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-terasort -n $namespace --ignore-not-found=true

printf "\n\nDELETING SPARK TERAVALIDATE OPERATOR\n\n"
kubectl delete sparkapplication spark-benchmark-teravalidate -n $namespace --ignore-not-found=true

printf "\n\nDELETING TEMP POD\n\n"
kubectl delete pod spark-benchmark-temp-pod -n $namespace --ignore-not-found=true

printf "\n\nDELETING PVC\n\n"
kubectl delete pvc spark-benchmark-claim -n $namespace --ignore-not-found=true

printf "\n\nDELETING MAPR TICKET\n\n"
kubectl delete secret jenkins-secret -n $namespace --ignore-not-found=true

namespace=$1

printf "\n\n\n\n\nDELETING SPARK JOIN OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication spark-benchmark-join -n $namespace

printf "\n\n\n\n\nDELETING SPARK TERAGEN OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication spark-benchmark-teragen -n $namespace

printf "\n\n\n\n\nDELETING SPARK TERSORT OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication spark-benchmark-terasort -n $namespace

printf "\n\n\n\n\nDELETING SPARK TERAVALIDATE OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication spark-benchmark-teravalidate -n $namespace

printf "\n\n\n\n\nDELETING TEMP POD\n\n\n\n\n"
kubectl delete pod spark-benchmark-temp-pod -n $namespace

printf "\n\n\n\n\nDELETING PVC\n\n\n\n\n"
kubectl delete pvc spark-benchmark-claim -n $namespace

printf "\n\n\n\n\nDELETING MAPR TICKET\n\n\n\n\n"
kubectl delete secret jenkins-secret -n $namespace

printf "\n\n\n\n\nDone .... :)\n\n\n\n\n"

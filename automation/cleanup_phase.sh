namespace=$1

printf "\n\n\n\n\nDELETING SPARK JOIN OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-join -n $namespace

printf "\n\n\n\n\nDELETING SPARK TERRAGEN OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-terragen -n $namespace

printf "\n\n\n\n\nDELETING SPARK TERRASORT OPERATOR\n\n\n\n\n"
kubectl delete sparkapplication.sparkoperator.k8s.io spark-benchmark-terrasort -n $namespace

printf "\n\n\n\n\nDELETING TEMP POD\n\n\n\n\n"
kubectl delete pod spark-benchmark-temp-pod -n $namespace

printf "\n\n\n\n\nDELETING PVC\n\n\n\n\n"
kubectl delete pvc spark-benchmark-claim -n $namespace

printf "\n\n\n\n\nDELETING MAPR TICKET\n\n\n\n\n"
kubectl delete secret jenkins-secret -n $namespace

printf "\n\n\n\n\nDone .... :)\n\n\n\n\n"

set -e

grpc_file="grpc_${1}.txt"
go run . -client-protocol grpc | tr 'ms' ' ' > $grpc_file 2>&1
gsutil cp $grpc_file gs://princer-working-dirs/

http_file="http_${1}.txt"
go run . -client-protocol http | tr 'ms' ' ' > $http_file 2>&1
gsutil cp $http_file gs://princer-working-dirs/

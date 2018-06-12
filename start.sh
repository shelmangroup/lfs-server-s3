export AWS_ACCESS_KEY_ID=reducted
export AWS_SECRET_ACCESS_KEY=reducted
export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_BUCKET=test
export AWS_REGION=eu-west-1
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://test
./lfs-test-server

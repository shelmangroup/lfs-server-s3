docker run --name minio -d -p 9000:9000 -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=miniostorage minio/minio server /data
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=miniostorage
export LFS_S3ENDPOINT=http://127.0.0.1:9000
export LFS_S3BUCKET=test
export LFS_S3REGION=eu-west-1
export LFS_ADMINUSER=admin
export LFS_ADMINPASS=admin
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://$LFS_S3BUCKET
../lfs-server-s3

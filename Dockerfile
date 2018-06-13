FROM debian:stable

COPY lfs-test-server /lfs-server

ENV AWS_ACCESS_KEY_ID minio
ENV AWS_SECRET_ACCESS_KEY miniostorage
ENV AWS_ENDPOINT http://127.0.0.1:9000
ENV AWS_BUCKET lfs-bucket
ENV AWS_REGION eu-west-1
ENV LFS_ADMINUSER admin
ENV LFS_ADMINPASS admin

EXPOSE 8080

ENTRYPOINT ["/lfs-server"]

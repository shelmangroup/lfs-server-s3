FROM golang:1.10 as build
WORKDIR /go/src/lfs-server-s3
ADD . /go/src/lfs-server-s3
RUN go install .

FROM gcr.io/distroless/base
COPY --from=build /go/bin/lfs-server-s3 /lfs-server
ENV AWS_ACCESS_KEY_ID minio
ENV AWS_SECRET_ACCESS_KEY miniostorage
ENV LFS_S3ENDPOINT http://127.0.0.1:9000
ENV LFS_S3BUCKET lfs-bucket
ENV LFS_S3REGION eu-west-1
ENV LFS_TRACEURL http://127.0.0.1:14268
ENV LFS_ADMINUSER admin
ENV LFS_ADMINPASS admin
EXPOSE 8080
ENTRYPOINT ["/lfs-server"]

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

var (
	blobPrefix = "blobs"
)

// ContentStore provides a simple file system based storage.
type S3ContentStore struct {
	session    *session.Session
	service    *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

// NewContentStore creates a ContentStore at the base directory.
func NewS3ContentStore() *S3ContentStore {
	log.WithFields(log.Fields{
		"bucket":   Config.S3Bucket,
		"endpoint": Config.S3Endpoint,
		"region":   Config.S3Region,
	}).Info("Creating AWS session for content store")

	awsLogger := log.WithField("component", "aws-sdk")

	awsConfig := &aws.Config{
		Region:   aws.String(Config.S3Region),
		Endpoint: aws.String(Config.S3Endpoint),
		Logger: aws.LoggerFunc(func(args ...interface{}) {
			awsLogger.Info(args...)
		}),
		S3ForcePathStyle: aws.Bool(true),
	}

	sess := session.Must(session.NewSession(awsConfig))

	return &S3ContentStore{
		session:    sess,
		service:    s3.New(sess),
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
	}
}

func (s *S3ContentStore) makeKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.ReaderCloser. If fromByte > 0, the reader starts from that byte
func (s *S3ContentStore) Get(meta *MetaObject, fromByte int64) (io.Reader, error) {
	key := s.makeKey(blobPrefix, transformKey(meta.Oid))

	buf := make([]byte, meta.Size)

	log.WithField("object", key).Info("Get")
	numBytes, err := s.downloader.Download(
		aws.NewWriteAtBuffer(buf),
		&s3.GetObjectInput{
			Bucket: aws.String(Config.S3Bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"bucket": Config.S3Bucket,
		"key":    key,
		"bytes":  numBytes,
	}).Info("Download complete")

	return bytes.NewReader(buf), nil
}

// Put takes a Meta object and an io.Reader and writes the content to the store.
func (s *S3ContentStore) Put(meta *MetaObject, r io.Reader) error {
	key := s.makeKey(blobPrefix, transformKey(meta.Oid))

	var buf bytes.Buffer

	digest := sha256.New()
	tee := io.TeeReader(r, &buf)

	written, err := io.Copy(digest, tee)
	if err != nil {
		return err
	}

	if written != meta.Size {
		return errSizeMismatch
	}

	shaStr := hex.EncodeToString(digest.Sum(nil))
	if shaStr != meta.Oid {
		return errHashMismatch
	}

	log.WithField("object", key).Info("Put")
	_, err = s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(Config.S3Bucket),
		Key:    aws.String(key),
		Body:   &buf,
	})
	if err != nil {
		return err
	}

	return nil
}

// Exists returns true if the object exists in the content store.
func (s *S3ContentStore) Exists(meta *MetaObject) bool {
	key := s.makeKey(blobPrefix, transformKey(meta.Oid))

	log.WithField("object", key).Info("HEAD")
	input := &s3.HeadObjectInput{
		Bucket: aws.String(Config.S3Bucket),
		Key:    aws.String(key),
	}
	_, err := s.service.HeadObject(input)
	if err != nil {
		return false
	}
	return true
}

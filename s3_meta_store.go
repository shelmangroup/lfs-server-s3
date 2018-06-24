package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

// MetaStore implements a metadata storage. It stores user credentials and Meta information
// for objects. The storage is handled by boltdb.
type S3MetaStore struct {
	session    *session.Session
	service    *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

var (
	errNotImplemeted = errors.New("Method not implemented")
	errNotOwner      = errors.New("Attempt to delete other user's lock")
)

var (
	usersPrefix   = "users"
	objectsPrefix = "objects"
	locksPrefix   = "locks"
)

// NewMetaStore creates a new MetaStore using the boltdb database at dbFile.
func NewS3MetaStore() *S3MetaStore {
	log.WithFields(log.Fields{
		"bucket":   Config.S3Bucket,
		"endpoint": Config.S3Endpoint,
		"region":   Config.S3Region,
	}).Info("Creating AWS session for meta store")

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

	return &S3MetaStore{
		session:    sess,
		service:    s3.New(sess),
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
	}
}

func (s *S3MetaStore) makeKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

// Get retrieves the Meta information for an object given information in
// RequestVars
func (s *S3MetaStore) Get(v *RequestVars) (*MetaObject, error) {
	meta, error := s.UnsafeGet(v)
	return meta, error
}

func (s *S3MetaStore) s3Get(key string) ([]byte, error) {
	buf := make([]byte, 1024*1024*4)

	log.WithField("object", key).Debug("Get")
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
	}).Debug("Download complete")

	return buf[:numBytes], nil
}
func (s *S3MetaStore) s3List(prefix string) ([]string, error) {
	pageNum := 0
	var keys []string
	err := s.service.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(Config.S3Bucket),
		Prefix: aws.String(prefix),
	}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
		pageNum++
		for _, obj := range p.Contents {
			log.WithFields(log.Fields{
				"bucket": Config.S3Bucket,
				"object": *obj.Key,
			}).Debug("list complete")
			keys = append(keys, *obj.Key)
		}
		return true
	})
	if err != nil {
		return keys, err
	}

	return keys, nil

}
func (s *S3MetaStore) s3Put(key string, data io.Reader) error {
	log.WithField("object", key).Debug("Put")
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(Config.S3Bucket),
		Key:    aws.String(key),
		Body:   data,
	})
	return err
}

func (s *S3MetaStore) s3Delete(key string) error {
	log.WithField("object", key).Debug("Delete")
	_, err := s.service.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(Config.S3Bucket),
		Key:    aws.String(key),
	})
	return err
}

// Get retrieves the Meta information for an object given information in
// RequestVars
// DO NOT CHECK authentication, as it is supposed to have been done before
func (s *S3MetaStore) UnsafeGet(v *RequestVars) (*MetaObject, error) {
	var meta MetaObject

	key := s.makeKey(objectsPrefix, v.Oid)

	buf, err := s.s3Get(key)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(bytes.NewBuffer(buf))
	err = dec.Decode(&meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

// Put writes meta information from RequestVars to the store.
func (s *S3MetaStore) Put(v *RequestVars) (*MetaObject, error) {
	// Check if it exists first
	if meta, err := s.Get(v); err == nil {
		meta.Existing = true
		return meta, nil
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	meta := MetaObject{Oid: v.Oid, Size: v.Size}
	err := enc.Encode(meta)
	if err != nil {
		return nil, err
	}

	key := s.makeKey(objectsPrefix, v.Oid)
	err = s.s3Put(key, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

// Delete removes the meta information from RequestVars to the store.
func (s *S3MetaStore) Delete(v *RequestVars) error {
	key := s.makeKey(objectsPrefix, v.Oid)
	return s.s3Delete(key)
}

type LocksByCreatedAt []Lock

func (c LocksByCreatedAt) Len() int           { return len(c) }
func (c LocksByCreatedAt) Less(i, j int) bool { return c[i].LockedAt.Before(c[j].LockedAt) }
func (c LocksByCreatedAt) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// AddLocks write locks to the store for the repo.
func (s *S3MetaStore) AddLocks(repo string, l ...Lock) error {
	key := s.makeKey(locksPrefix, repo)
	var locks []Lock
	data, _ := s.s3Get(key)
	if data != nil {
		if err := json.Unmarshal(data, &locks); err != nil {
			return err
		}
	}
	locks = append(locks, l...)
	sort.Sort(LocksByCreatedAt(locks))
	data, err := json.Marshal(&locks)
	if err != nil {
		return err
	}

	return s.s3Put(key, bytes.NewReader(data))
}

// Locks retrieves locks for the repo from the store
func (s *S3MetaStore) Locks(repo string) ([]Lock, error) {
	key := s.makeKey(locksPrefix, repo)
	var locks []Lock
	data, _ := s.s3Get(key)
	if data != nil {
		if err := json.Unmarshal(data, &locks); err != nil {
			return locks, err
		}
	}
	return locks, nil
}

// FilteredLocks return filtered locks for the repo
func (s *S3MetaStore) FilteredLocks(repo, path, cursor, limit string) (locks []Lock, next string, err error) {
	locks, err = s.Locks(repo)
	if err != nil {
		return
	}

	if cursor != "" {
		lastSeen := -1
		for i, l := range locks {
			if l.Id == cursor {
				lastSeen = i
				break
			}
		}

		if lastSeen > -1 {
			locks = locks[lastSeen:]
		} else {
			err = fmt.Errorf("cursor (%s) not found", cursor)
			return
		}
	}

	if path != "" {
		var filtered []Lock
		for _, l := range locks {
			if l.Path == path {
				filtered = append(filtered, l)
			}
		}

		locks = filtered
	}

	if limit != "" {
		var size int
		size, err = strconv.Atoi(limit)
		if err != nil || size < 0 {
			locks = make([]Lock, 0)
			err = fmt.Errorf("Invalid limit amount: %s", limit)
			return
		}

		size = int(math.Min(float64(size), float64(len(locks))))
		if size+1 < len(locks) {
			next = locks[size].Id
		}
		locks = locks[:size]
	}

	return locks, next, nil
}

// DeleteLock removes lock for the repo by id from the store
func (s *S3MetaStore) DeleteLock(repo, user, id string, force bool) (*Lock, error) {
	var deleted *Lock
	var locks []Lock

	key := s.makeKey(locksPrefix, repo)

	data, err := s.s3Get(key)
	if err != nil {
		return deleted, err
	}
	if data != nil {
		if err := json.Unmarshal(data, &locks); err != nil {
			return deleted, err
		}
	}
	newLocks := make([]Lock, 0, len(locks))

	var lock Lock
	for _, l := range locks {
		if l.Id == id {
			if l.Owner.Name != user && !force {
				return deleted, errNotOwner
			}
			lock = l
		} else if len(l.Id) > 0 {
			newLocks = append(newLocks, l)
		}
	}
	if lock.Id == "" {
		return deleted, nil
	}
	deleted = &lock

	if len(newLocks) == 0 {
		return deleted, s.s3Delete(key)
	}

	data, err = json.Marshal(&newLocks)
	if err != nil {
		return deleted, err
	}
	err = s.s3Put(key, bytes.NewReader(data))

	return deleted, err
}

// Close closes the underlying s3 manager
func (s *S3MetaStore) Close() {
	return
}

// AddUser adds user credentials to the meta store.
func (s *S3MetaStore) AddUser(user, pass string) error {
	return errNotImplemeted
}

// DeleteUser removes user credentials from the meta store.
func (s *S3MetaStore) DeleteUser(user string) error {
	return errNotImplemeted
}

// MetaUser encapsulates information about a meta store user
type S3MetaUser struct {
	Name string
}

// Users returns all MetaUsers in the meta store
func (s *S3MetaStore) Users() ([]*S3MetaUser, error) {
	return nil, errNotImplemeted
}

// Objects returns all MetaObjects in the meta store
func (s *S3MetaStore) Objects() ([]*MetaObject, error) {
	var objects []*MetaObject

	keys, err := s.s3List(objectsPrefix)
	if err != nil {
		return objects, err
	}
	for _, k := range keys {
		var meta MetaObject
		data, err := s.s3Get(k)
		if err != nil {
			return objects, err
		}
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		err = dec.Decode(&meta)
		if err != nil {
			return objects, err
		}
		objects = append(objects, &meta)
	}
	return objects, err
}

// AllLocks return all locks in the store, lock path is prepended with repo
func (s *S3MetaStore) AllLocks() ([]Lock, error) {
	var locks []Lock
	keys, err := s.s3List(locksPrefix)
	if err != nil {
		return locks, err
	}

	for _, k := range keys {
		var l []Lock
		data, err := s.s3Get(k)
		if err != nil {
			return locks, err
		}
		if err := json.Unmarshal(data, &l); err != nil {
			return locks, err
		}
		for _, lv := range l {
			lv.Path = fmt.Sprintf("%s:%s", k, lv.Path)
			locks = append(locks, lv)
		}
	}
	return locks, err
}

// Authenticate authorizes user with password and returns the user name
func (s *S3MetaStore) Authenticate(user, password string) (string, bool) {
	return "", true
}

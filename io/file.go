package io

import (
	"cloud.google.com/go/storage"
	"context"
	"io/ioutil"
	"os"
)

type FileIO interface {
	Read (obj string,ctx context.Context) ([]byte, error)
	Write (obj string, payload []byte, ctx context.Context) error
}

type GcsIO struct {
	BucketHandle *storage.BucketHandle
}

type LocalIO struct {

}

func (lio *LocalIO) Write(obj string, payload []byte,ctx context.Context) error {
	perm := os.FileMode(0644)
	err := ioutil.WriteFile(obj, payload, perm)
	if err != nil {
		return err
	}

	return nil
}

func (lio *LocalIO) Read(obj string, ctx context.Context) ([]byte, error) {
	buf, err := ioutil.ReadFile(obj)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (gio *GcsIO) Write(obj string, payload []byte,ctx context.Context) error {
	writer := gio.BucketHandle.Object(obj).NewWriter(ctx)
	defer writer.Close()
	_, err := writer.Write(payload)

	if err != nil {
		return err
	}

	return nil
}

func (gio *GcsIO) Read(obj string, ctx context.Context) ([]byte, error) {
	reader, err := gio.BucketHandle.Object(obj).NewReader(ctx)
	defer reader.Close()
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

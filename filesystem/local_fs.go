package filesystem

import (
	"io"
	"os"
	"path/filepath"
)

type localFSClient struct {
}

func NewLocalFSClient() Client {
	return &localFSClient{}
}

func (c *localFSClient) OpenReadCloser(name string) (io.ReadCloser, error) {
	return os.Open(name)
}

func (c *localFSClient) OpenWriteCloser(name string) (io.WriteCloser, error) {
	exist, err := c.Exists(name)
	if err != nil {
		return nil, err
	}
	if !exist {
		f, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	return os.OpenFile(name, os.O_WRONLY, 0)
}

func (c *localFSClient) Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	return existCommon(err)
}

func (c *localFSClient) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (c *localFSClient) Glob(pattern string) (matches []string, err error) {
	return filepath.Glob(pattern)
}

func (c *localFSClient) Remove(name string) error {
	return os.Remove(name)
}

func existCommon(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

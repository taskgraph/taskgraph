package filesystem

import (
	"io"
	"os"
	"path"
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
	return os.Open(name)
}

func (c *localFSClient) Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	return existCommon(err)
}

func (c *localFSClient) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (c *localFSClient) Glob(dirname, pattern string) ([]string, error) {
	return filepath.Glob(path.Join(dirname, pattern))
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

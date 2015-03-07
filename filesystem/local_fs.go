package filesystem

import (
	"os"
	"path/filepath"
)

type localFSClient struct {
}

func NewLocalFSClient() Client {
	return &localFSClient{}
}

func (c *localFSClient) Create(name string) (File, error) {
	return os.Create(name)
}

func (c *localFSClient) Open(name string) (File, error) {
	return os.Open(name)
}

func (c *localFSClient) Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	return existCommon(err)
}

func (c *localFSClient) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (c *localFSClient) GlobPrefix(prefix string) ([]string, error) {
	return filepath.Glob(prefix)
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

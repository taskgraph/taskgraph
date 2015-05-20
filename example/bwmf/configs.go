package bwmf

import (
	"encoding/json"
)

type optconfig struct {
	Sigma float32
	Alpha float32
	Beta  float32

	GradTol  float32
	FixedCnt int

	// num of iters for the alternating optimization
	NumIters  uint64
	DimLatent uint32
}

// xFs can be "local", ""
type ioconfig struct {
	Fs        string
	IDPath    string
	ITPath    string
	ODPath    string
	OTPath    string
	InitDPath string
	InitTPath string

	HdfsConf  hdfsConfig
	AzureConf azureConfig
}

type Config struct {
	OptConf optconfig
	IOConf  ioconfig
}

type hdfsConfig struct {
	NamenodeAddr string
	WebHdfsAddr  string
	User         string
}

type azureConfig struct {
	AccountName        string
	AccountKey         string
	BlogServiceBaseUrl string
	ApiVersion         string
	UseHttps           bool
}

func Parse(buf []byte) (*Config, error) {
	conf := &Config{}
	err := json.Unmarshal(buf, conf)
	return conf, err
}

func Dump(conf *Config) ([]byte, error) {
	return json.Marshal(conf)
}

package bwmf

import (
	"fmt"
	"os"
	"testing"
)

func TestParseDump(t *testing.T) {
	config := &Config{
		OptConf: optconfig{
			Sigma:   0.01,
			Alpha:   1.0,
			Beta:    0.1,
			GradTol: 1e-6,
		},
		IOConf: ioconfig{
			IFs:     "local",
			IDPath:  "./row_shard.dat",
			ITPath:  "./column_shard.dat",
			OFs:     "local",
			ODPath:  "./dShard.dat",
			OTPath:  "./tShard.dat",
			HdfsConf: hdfsConfig{
				NamenodeAddr: "name node addr",
				WebHdfsAddr:  "web hdfs addr",
				User:         "user",
			},
			AzureConf: azureConfig{
				AccountName:        "account name",
				AccountKey:         "account key",
				BlogServiceBaseUrl: "blog service base url",
				UseHttps:           true,
			},
		},
	}

	fmt.Println("Original config is: ", *config)
	buf, dErr := Dump(config)
	if dErr != nil {
		t.Errorf("Failed dumping. err: %s", dErr)
	}
	// fmt.Println("Marshalled buf is: ", buf)
	os.Stdout.Write(buf)
	fmt.Println("\n")
	newConfig, pErr := Parse(buf)
	if pErr != nil {
		t.Errorf("Failed parsing. err: %s", pErr)
	}

	fmt.Println("Parsed config is: ", *newConfig)
	if newConfig.IOConf.IFs != config.IOConf.IFs {
		t.Errorf("ioConf.iFs not equal.")
	}

	if newConfig.OptConf.Alpha != config.OptConf.Alpha {
		t.Errorf("optConf.alpha not equal.")
	}

	if newConfig.IOConf.HdfsConf.User != config.IOConf.HdfsConf.User {
		t.Errorf("IOConf.HdfsConf.User not equal.")
	}

	if newConfig.IOConf.AzureConf.ApiVersion != config.IOConf.AzureConf.ApiVersion {
		t.Errorf("IOConf.AzureConf.ApiVersion not equal.")
	}

}

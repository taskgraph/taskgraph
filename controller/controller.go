package controller

import (
	"log"
	"os"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

// This is the controller of a job.
// A job needs controller to setup etcd data layout, request
// cluster containers, etc. to setup framework to run.
type Controller struct {
	name           string
	etcdclient     *etcd.Client
	numOfTasks     uint64
	failDetectStop chan bool
	logger         *log.Logger
	jobStatusChan  chan string
	linkTypes      []string
}

func New(name string, etcd *etcd.Client, numOfTasks uint64, pLinkTypes []string) *Controller {
	return &Controller{
		name:       name,
		etcdclient: etcd,
		numOfTasks: numOfTasks,
		logger:     log.New(os.Stdout, "", log.Lshortfile|log.Ltime|log.Ldate),
		linkTypes:  pLinkTypes,
	}
}

// A controller typical workflow:
// 1. controller sets up etcd layout before any task starts running.
// 2. Being ready, controller lets other tasks to run and reports any failure found.
func (c *Controller) Start() error {
	if err := c.InitEtcdLayout(); err != nil {
		return err
	}
	// Currently no previous changes will be watches before watch is setup.
	// We assumes that ttl is usually a few seconds. watch is setup before that.
	go c.startFailureDetection()
	c.logger.Printf("Controller starting, name: %s, numberOfTask: %d\n", c.name, c.numOfTasks)
	return nil
}

func (c *Controller) WaitForJobDone() error {
	<-c.jobStatusChan
	return nil
}

func (c *Controller) Stop() error {
	c.DestroyEtcdLayout()
	c.stopFailureDetection()
	c.logger.Printf("Controller stoping...\n")
	return nil
}

func (c *Controller) InitEtcdLayout() error {
	// Initilize the job epoch to 0
	etcdutil.MustCreate(c.etcdclient, c.logger, etcdutil.EpochPath(c.name), "0", 0)
	c.setupWatchOnJobStatus()
	// initiate etcd data layout for tasks
	// currently it creates as many unassigned tasks as task masters.
	for i := uint64(0); i < c.numOfTasks; i++ {
		key := etcdutil.FreeTaskPath(c.name, strconv.FormatUint(i, 10))
		etcdutil.MustCreate(c.etcdclient, c.logger, key, "", 0)
		for _, linkType := range c.linkTypes {
			key = etcdutil.MetaPath(linkType, c.name, i)
			etcdutil.MustCreate(c.etcdclient, c.logger, key, "", 0)
		}
	}
	return nil
}

func (c *Controller) DestroyEtcdLayout() error {
	_, err := c.etcdclient.Delete("/", true)
	return err
}

func (c *Controller) startFailureDetection() error {
	c.failDetectStop = make(chan bool, 1)
	err := etcdutil.DetectFailure(c.etcdclient, c.name, c.failDetectStop, c.logger)
	if err != nil {
		// We currently didn't handle outside. So we do some logging at least.
		c.logger.Printf("DetectFailure returns error: %v", err)
	}
	return err
}

func (c *Controller) setupWatchOnJobStatus() {
	c.jobStatusChan = make(chan string, 1)
	key := etcdutil.JobStatusPath(c.name)
	resp := etcdutil.MustCreate(c.etcdclient, c.logger, key, "", 0)
	go func() {
		resp, err := c.etcdclient.Watch(key, resp.EtcdIndex+1, false, nil, nil)
		if err != nil {
			c.logger.Panicf("Watch on job status (%v) failed: %v", key, err)
		}
		c.jobStatusChan <- resp.Node.Value
	}()
}

func (c *Controller) stopFailureDetection() error {
	c.failDetectStop <- true
	return nil
}

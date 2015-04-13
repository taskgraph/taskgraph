import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type shuffleTask struct {
	epoch uint64
	framework taskgraph.framework
	numOfMapper uint64
	desReducerTaskId uint64

}

func (sf *shuffleTask) Init(taskId uint64, framework taskgraph.Framework) {
	sf.taskID
}

func (sf *shuffleTask) {
	for {
		select {
			case ec := <-sf.epochChange
			sf.doEnterEpoch(sf.ctx, sf.epoch)

			case shuffleDone := <- shuffleDone
			mp.framework.FlagMeta(mapperDone, "Suffix", "metaReady")
		}
	}
}


func read

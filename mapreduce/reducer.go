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
struct reducerTask {
	framework taskgraph.framework
	epoch uint64
	log *log.logger
	taskId uint64
	numOfTasks uint64

}

func (rd *mapperTask) Init(taskId uint64, framework taskgraph.Framework) {
	rd.taskID = taskID
	rd.framework = framework
	
}
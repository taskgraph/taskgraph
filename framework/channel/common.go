package channel

import "fmt"

type channelCommon struct {
	taskID uint64
	tag    string
}

func (c channelCommon) TaskID() uint64 {
	return c.taskID
}

func (c channelCommon) Tag() string {
	return c.tag
}

func (c channelCommon) ID() string {
	return fmt.Sprintf("%d-%s", c.taskID, c.tag)
}

package channel

import "testing"

// This test simulates regression framework joints uses.

// A task can send a slave some parameter, and the slave have two joints that read
// the same parameter.
func TestParameterFanOut(t *testing.T) {
	c := NewClient()
	s := NewServer()
	out := NewOutbound()
	s.AttachOutbound(out)
	in1 := NewInbound()
	c.AttachInbound(in1)
	in2 := NewInbound()
	c.AttachInbound(in2)
	c.SetupDispatch()

	go out.Put(new(dummyData))

	if string(in1.Get()) != "taskgraph rocks" {
		t.Errorf("inbound 1 gets = %s, wants = %s", string(in1.Get()), "taskgraph rocks")
	}
	if string(in2.Get()) != "taskgraph rocks" {
		t.Errorf("inbound 2 gets = %s, wants = %s", string(in2.Get()), "taskgraph rocks")
	}
}

type dummyData struct {
}

func (*dummyData) Marshal() ([]byte, error) {
	return []byte("taskgraph rocks"), nil
}

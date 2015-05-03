package common

import "sync"

// I am writing this count down latch because sync.WaitGroup doesn't support
// decrementing counter when it's 0.
type CountdownLatch struct {
	sync.Mutex
	cond    *sync.Cond
	counter int
}

func NewCountdownLatch(count int) *CountdownLatch {
	c := new(CountdownLatch)
	c.cond = sync.NewCond(c)
	c.counter = count
	return c
}

func (c *CountdownLatch) Count() int {
	c.Lock()
	defer c.Unlock()
	return c.counter
}

func (c *CountdownLatch) CountDown() {
	c.Lock()
	defer c.Unlock()
	if c.counter == 0 {
		return
	}
	c.counter--
	if c.counter == 0 {
		c.cond.Broadcast()
	}
}

func (c *CountdownLatch) Await() {
	c.Lock()
	defer c.Unlock()
	if c.counter == 0 {
		return
	}
	c.cond.Wait()
}

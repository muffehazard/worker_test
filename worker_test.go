package worker_test

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/muffehazard/worker"
)

type ChannelReturner struct {
	retChan chan int
	retVal  int
}

func (r *ChannelReturner) Do() error {
	r.retChan <- r.retVal
	return nil
}

func (r *ChannelReturner) Describe() string {
	return fmt.Sprintf("ChannelReturner %v", r.retVal)
}

func TestJobOrder(t *testing.T) {
	tests := 1000000
	w := worker.NewWorker(nil)
	in := make(chan int)

	go func() {
		for i := 1; i <= tests; i++ {
			_, w = w.NextWorker(&ChannelReturner{
				retChan: in,
				retVal:  i,
			})
		}
	}()

	recv := 0
	for {
		select {
		case i := <-in:
			if recv > i {
				t.Errorf("Jobs out of order: %v > %v", recv, i)
			} else if i == tests {
				w.Kill()
				return
			}
			recv = i
		}
	}
}

type GenericJob struct{}

func (j *GenericJob) Do() error {
	return nil
}

func (j *GenericJob) Describe() string {
	return "Generic"
}

func TestCountChan(t *testing.T) {
	tests := 10
	started := 0
	returned := 0
	cChan := make(chan int)
	w := worker.NewWorker(&worker.WorkerOpts{
		CountChan: cChan,
	})

	go func() {
		for i := 0; i < tests; i++ {
			_, w = w.NextWorker(&GenericJob{})
			started++
		}
	}()

	for {
		select {
		case job := <-cChan:
			returned++
			log.Printf("Job %v returned", job)
			log.Printf("Jobs in queue: %v", started-returned)
			if returned == started {
				return
			}
		}
	}
}

type TimerJob struct {
	WaitTime time.Duration
}

func (j *TimerJob) Do() error {
	time.Sleep(j.WaitTime)
	log.Printf("TimerJob done: %v", j.WaitTime)
	return nil
}

func (j *TimerJob) Describe() string {
	return fmt.Sprintf("Timer: %v", j.WaitTime)
}

func TestTimeout(t *testing.T) {
	tests := 10
	returned := 0
	seconds := time.Duration(5)
	timeout := seconds * time.Second
	cChan := make(chan int)
	w := worker.NewWorker(&worker.WorkerOpts{
		CountChan: cChan,
		Timeout:   timeout,
	})

	for i := 0; i < tests; i++ {
		r := time.Duration(rand.Int()) % (seconds * 2) * time.Second
		_, w = w.NextWorker(&TimerJob{
			WaitTime: r,
		})
	}

	for {
		select {
		case err := <-w.ErrChan:
			log.Printf("Err: %v", err)
			switch e := err.(type) {
			case worker.JobTimeoutErr:
				go func() {
					err := <-e.ErrorChan()
					log.Printf("Timed out returned: %v", err)
				}()
			}
		case jobId := <-cChan:
			returned++
			log.Printf("Job %v returned", jobId)
			if returned == tests {
				return
			}
		}
	}
}

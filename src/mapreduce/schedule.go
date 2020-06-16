package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	DoTaskRPC := "Worker.DoTask"
	var wg sync.WaitGroup
	producer := make(chan *DoTaskArgs, ntasks)

	done := make(chan bool)
	timer := make(chan int)
	for i := 0; i < ntasks; i++ {
		var taskArgs *DoTaskArgs
		switch phase {
		case mapPhase:
			taskArgs = &DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nios,
			}
		case reducePhase:
			taskArgs = &DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nios,
			}
		}
		producer <- taskArgs
		wg.Add(1)
	}

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		done <- true
	}(&wg)

	go func() {
		for {
			time.Sleep(5 * time.Second)
			timer <- 1
		}
	}()

work:
	for {
		select {
		case <-done:
			// fmt.Println("done")
			break work

		case worker := <-mr.registerChannel:
			// fmt.Println("Consume worker")
			select {
			case taskArgs := <-producer:
				fmt.Printf("Calling worker: %s\n", worker)
				go func(wg *sync.WaitGroup) {
					ok := call(worker, DoTaskRPC, taskArgs, new(struct{}))

					// return worker if the task completed
					// else we will ignore that worker for future tasks.
					if ok {
						wg.Done()
						// fmt.Println("Worker returned")
						mr.registerChannel <- worker
					} else {
						// fmt.Printf("%s: RPC %s error\n", DoTaskRPC, worker)

						// always return the task back to the producer queue if
						// it is not completed.
						producer <- taskArgs
					}
					// fmt.Println("Worker routine ended")
				}(&wg)
			default:
				// fmt.Println("return worker")
				go func() {
					mr.registerChannel <- worker
				}()
				// fmt.Println("returned worker")
			}

		default:
			// case <-timer:
			// fmt.Println("no worker/not done")
		}

	}
	debug("Schedule: %v phase done\n", phase)
}

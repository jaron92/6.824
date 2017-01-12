package mapreduce

import (
	"fmt"
	// "time"
)


var taskArgsChannel = make(chan *DoTaskArgs)
var taskDoneChannel = make(chan int)
var waitingChannel = make(chan int)

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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	// time.Sleep(10 * time.Millisecond)

	// handle register message
	go func() {
		for worker := range mr.registerChannel {
			go func(){
				waitingChannel <- 1
			}()
			go func(wk string) {
				// issue task
				for taskArgs := range taskArgsChannel {
					ok := call(wk, "Worker.DoTask", taskArgs, new(struct{}))
					if ok == false {
						fmt.Printf("DoTask: RPC %s DoTask error\n", wk)
						fmt.Printf("Try to re-assign the task to workers\n")
						go func() {
							taskArgsChannel <- taskArgs
						}()
						break
					} else {
						go func(){
							taskDoneChannel <- 1
						}()
					}
				}
			}(worker)
		}
	}()

	// waiting until any register message arrives
	if cap(mr.workers) == 0 {
		<- waitingChannel
	}

	for i := 0; i < ntasks; i++ {
		taskArgs := new(DoTaskArgs)
		taskArgs.JobName = mr.jobName
		if phase == mapPhase {
			taskArgs.File = mr.files[i]
		}
		taskArgs.Phase = phase
		taskArgs.TaskNumber = i
		taskArgs.NumOtherPhase = nios
		taskArgsChannel <- taskArgs
	}

	// waiting until all tasks are done
	for i := 0; i < ntasks; i++ {
		<- taskDoneChannel
	}


	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

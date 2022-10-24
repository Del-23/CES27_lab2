package mapreduce

import (
	"log"
	"sync"
	"sync/atomic"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		wg        sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *Operation
		counter   uint64
	)

	log.Printf("Scheduling %v operations\n", proc)

	counter = 0
	for filePath = range filePathChan {
		operation = &Operation{proc, int(counter), filePath}
		counter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg, 0)
	}

	wg.Wait()

	for failedOp := range master.failedOpsChan {
		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, failedOp, &wg, counter)
	}

	wg.Wait()

	log.Printf("%vx %v operations completed\n", counter, proc)
	return int(counter)
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup, totalOps uint64) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))
	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		wg.Done()
		master.failedWorkerChan <- remoteWorker
		master.failedOpsChan <- operation
	} else {
		wg.Done()
		master.idleWorkerChan <- remoteWorker
		atomic.AddUint64(&master.counterSucceededOps, 1)
		counterSucceeded := atomic.LoadUint64(&master.counterSucceededOps)
		if totalOps != 0 && counterSucceeded == totalOps {
			close(master.failedOpsChan)
		}
	}
}

package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// runs the server with an actor
func rpcServer(masterAddress string, tasks *Tasks) *Server {
	var actor Server
	actor = startActor(tasks)
	rpc.Register(actor)
	rpc.HandleHTTP()
	// go function to serve RPC requests
	go func() {
		conn, err := net.Listen("tcp", masterAddress)
		if err != nil {
			log.Fatalf("listen error: %v", err)
		}
		if err := http.Serve(conn, nil); err != nil {
			log.Fatalf("http.Serve: %v", err)
		}
	}()
	return &actor
}

// creates an actor to handle functions
func startActor(tasks *Tasks) Server {
	var ch Server
	ch = make(chan handler)
	// printTasks(tasks)
	go func() {
		for f := range ch {
			f(tasks)
		}
	}()
	return ch
}

func call(address string, method string, request interface{}, response interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Printf("rpc.DialHTTP: %v", err)
		return err
	}
	defer client.Close()

	if err = client.Call(method, request, response); err != nil {
		log.Printf("client.Call %s: %v", method, err)
		return err
	}

	return nil
}

// Notifies the master of it's existence
func (s Server) Ping(junk *Nothing, rubbish *Nothing) error {
	finished := make(chan struct{})
	s <- func(t *Tasks) {
		fmt.Printf("Pinged by new Worker!\n")
		// adds to a count of workers
		t.AliveWorkers += 1
		finished <- struct{}{}
	}
	<-finished
	return nil
}

// Looks for a task that hasn't been distributed yet (DISTRIBUTED != FINISHED)
func (s Server) GetTask(junk *Nothing, task *Task) error {
	finished := make(chan struct{})
	s <- func(t *Tasks) {

		// Is a MapTask still available?
		for r, mT := range t.MTasks {
			if !mT.Distributed {
				t.MTasks[r].Distributed = true
				task.MTask = t.MTasks[r]
				task.GotATask = true
				task.IsMap = true
				break
			}
		}

		// Are the MapTasks finished?
		mapsFinished := true
		for _, mT := range t.MTasks {
			if !mT.Finished {
				mapsFinished = false
			}
		}

		// IF MapTasks are finished
		// Is there any Reduce Tasks available?
		if mapsFinished && !task.GotATask {
			for r, rT := range t.RTasks {
				if !rT.Distributed {
					t.RTasks[r].Distributed = true
					task.RTask = t.RTasks[r]
					task.GotATask = true
					break
				}
			}
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) ShutdownRequest(junk *Nothing, shutdown *Shutdown) error {
	finished := make(chan struct{})
	s <- func(t *Tasks) {

		// if program finished, shutdown is okay
		shutdown.Ok = t.Finished
		if t.Finished {
			t.AliveWorkers -= 1
		}

		finished <- struct{}{}
	}
	<-finished
	return nil
}

// Let the Master know that a Map Task has been completed
func (s Server) NotifyMapFinished(notification *Notification, junk *Nothing) error {
	finished := make(chan struct{})
	s <- func(t *Tasks) {

		// set the task to finished and update the reduce sources
		fmt.Printf("Maptask %d finished by %s\n", notification.TaskN, notification.Address)
		t.MTasks[notification.TaskN].Finished = true
		for _, task := range t.RTasks {
			task.SourceHosts[notification.TaskN] = notification.Address
			task.SourcePorts[notification.TaskN] = notification.Port
		}

		finished <- struct{}{}
	}
	<-finished
	return nil
}

// Let the Master know that a Reduce Task has been completed
func (s Server) NotifyReduceFinished(notification *Notification, junk *Nothing) error {
	finished := make(chan struct{})
	s <- func(t *Tasks) {

		// sets the task to finished and records what worker finished it (address and port)
		fmt.Printf("Reducetask %d finished by %s\n", notification.TaskN, notification.Address)
		t.RTasks[notification.TaskN].Finished = true
		t.RTasks[notification.TaskN].FinishedBy = notification.Address
		t.RTasks[notification.TaskN].FinishedByPort = notification.Port

		// Check to see if all ReduceTasks are finished
		allFinished := true
		for _, task := range t.RTasks {
			if !task.Finished {
				allFinished = false
			}
		}

		// IF they're all finished, we can merge the Reduce Outputs into our final output file
		if allFinished {
			var databaseUrls []string
			for i, task := range t.RTasks {
				databaseUrls = append(databaseUrls, makeURL(task.FinishedBy, task.FinishedByPort, reduceOutputFile(i)))
			}
			mergeDatabases(databaseUrls, "finalOutput.db", makeTempDir(t.MTasks[0].SourcePort)+"finalOutputTemp.db")
			fmt.Printf("finalOutput.db Created!\n")
			
			// t.Finished is for workers when they RPC ShutdownOk
			t.Finished = true
			// t.FinChannel is for the master to know that Merge is complete, it just has to wait for workers to shut down
			*t.FinChannel <- *junk
		}

		finished <- struct{}{}
	}
	<-finished
	return nil
}

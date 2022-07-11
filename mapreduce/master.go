package mapreduce

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
	"path"
)

type Tasks struct {
	MTasks       []MapTask
	RTasks       []ReduceTask
	Finished     bool
	AliveWorkers int
	FinChannel   *chan Nothing
}

type Task struct {
	RTask    ReduceTask
	MTask    MapTask
	IsMap    bool
	GotATask bool
}

type Shutdown struct {
	Ok bool
}

type Notification struct {
	TaskN   int
	Address string
	Port    string
}

/*
EXAMPLES OF CURL COMMAND:
	- curl http://192.168.0.241:3410/data/tmp3410/map_0_source.db
	- curl http://192.168.0.241:3410/data/austen.db
*/

func Start(client Interface) error {

	var isMaster bool
	var M, R int
	var port, masterAddress, tempDir string

	// runs command-input type of startup
	getInput(&isMaster, &M, &R, &port, &masterAddress, &tempDir)

	if isMaster { // Master
		if err := runMaster(M, R, port, tempDir); err != nil {
			log.Fatalf("error during master run: %v", err)
		}
	} else { // Worker
		if err := runWorker(port, masterAddress, tempDir, client); err != nil {
			log.Fatalf("error during worker run: %v", err)
		}
	}

	return nil
}

func runMaster(M, R int, port, tempDir string) error {
	// Get Address
	masterAddress := "localhost:" + port
	fmt.Printf("\nStarting master at address: %s\n", masterAddress)

	// get current directory
	currDirectory, err := os.Getwd()
	if err != nil {
		log.Fatalf("error getting current directory: %v", err)
	}

	// make temporary path
	dataPath := path.Join(currDirectory, "data")
	if err := os.MkdirAll(dataPath+"/"+tempDir, 0755); err != nil {
		log.Fatalf("error making directory in Master: %v", err)
	}

	// Delete last output file
	if err := os.Remove(dataPath + "/finalOutput.db"); err != nil {
		fmt.Printf("Failed to delete previous finalOutput.db: %v\n", err)
	}
	defer os.RemoveAll(dataPath + "/" + tempDir)

	// split input file
	splitInputFile(M, "data/austen.db", "data/"+tempDir)

	// generate map/reduce tasks
	// finChannel indicates finishing of the entire mapreduce process
	finChannel := make(chan Nothing)
	tasksMaster := Tasks{MTasks: make([]MapTask, M), RTasks: make([]ReduceTask, R), FinChannel: &finChannel}
	for i := 0; i < M; i++ {
		mTask := MapTask{M: M, R: R, N: i, SourceHost: masterAddress, Finished: false, SourcePort: port}
		tasksMaster.MTasks[i] = mTask
	}
	for i := 0; i < R; i++ {
		rTask := ReduceTask{M: M, R: R, N: i, Finished: false, SourceHosts: make([]string, M), SourcePorts: make([]string, M)}
		tasksMaster.RTasks[i] = rTask
	}

	// host http file server
	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(dataPath))))
		if err := http.ListenAndServe(masterAddress, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", masterAddress, err)
		}
	}()

	// start RPC server with actor
	_ = rpcServer(masterAddress, &tasksMaster)

	fmt.Printf("Server Now Online!\n\n")

	// recieve inidcation of finished program from finChannel, wait to shut down so the workers have time
	<-*tasksMaster.FinChannel
	time.Sleep(time.Second * 3)
	fmt.Printf("\n\nMapReduce Finished! Shutting Down...\n\n")
	return nil
}

func runWorker(port, masterAddress, tempDir string, client Interface) error {
	// get address for worker
	currentAddress := getLocalAddress() + ":" + port
	fmt.Printf("\nStarting worker at address: %s\n\n", currentAddress)

	// get current directory
	currDirectory, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error getting current directory: %v", err)
	}

	// create temporary directory
	dataPath := path.Join(currDirectory, "data")
	os.MkdirAll(dataPath+"/"+tempDir, 0755)
	defer os.RemoveAll(dataPath + "/" + tempDir)

	// host http file server
	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(dataPath))))
		if err := http.ListenAndServe(currentAddress, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v\n", currentAddress, err)
		}
	}()

	// initialize shutdown object and notify server of existence
	shutdown := Shutdown{Ok: false}
	var junk Nothing
	if err := call(masterAddress, "Server.Ping", &junk, &junk); err != nil {
		log.Fatalf("Failed to get task: %v", err)
	}

	// Run this loop while shutdown.Ok is false (Master has not indicated to shutdown)
	// PreviouslySlept is a way to make it so that the waiting for Master message doesn't flood the console
	previouslySlept := false
	for !shutdown.Ok {

		// Get a task
		var junk Nothing
		task := Task{}
		if err := call(masterAddress, "Server.GetTask", &junk, &task); err != nil {
			log.Fatalf("Failed to get task: %v", err)
		}

		// Process as either a Map task or a Reduce task
		if task.GotATask && task.IsMap {
			previouslySlept = false
			fmt.Printf("MapTask %d Recieved.\nProcessing... \n", task.MTask.N)
			task.MTask.Process(tempDir, client)
			fmt.Printf("Finished.\n\n")

			notification := Notification{TaskN: task.MTask.N, Address: currentAddress, Port: port}
			if err := call(masterAddress, "Server.NotifyMapFinished", &notification, &junk); err != nil {
				log.Fatalf("Failed to NotifyMapFinished: %v", err)
			}
		} else if task.GotATask {
			previouslySlept = false
			fmt.Printf("ReduceTask %d Recieved.\nProcessing... \n", task.RTask.N)
			time.Sleep(time.Duration(10000*task.RTask.N))
			task.RTask.Process(tempDir, client)
			fmt.Printf("Finished.\n\n")

			notification := Notification{TaskN: task.RTask.N, Address: currentAddress, Port: port}
			if err := call(masterAddress, "Server.NotifyReduceFinished", &notification, &junk); err != nil {
				log.Fatalf("Failed to NotifyMapFinished: %v", err)
			}

			// did not recieve a task - sleep this loop
		} else {
			if !previouslySlept {
				fmt.Printf("Did not recieve a task. Waiting On Master...\n\n")
			}
			time.Sleep(time.Second)
			previouslySlept = true
		}

		// Check to see if it is okay to shut down
		if err := call(masterAddress, "Server.ShutdownRequest", &junk, &shutdown); err != nil {
			log.Fatalf("Failed to request shutdown: %v", err)
		}
	}

	fmt.Printf("Shutting down...\n")
	return nil
}

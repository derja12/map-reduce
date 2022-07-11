package mapreduce

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
)

type MapTask struct {
	M, R        int    // number of map/reduce tasks
	N           int    // n'th map task (assigned number)
	SourceHost  string // address of host of input file
	SourcePort  string // port of host of the input
	Distributed bool
	Finished    bool
}

type ReduceTask struct {
	M, R           int      // number of map/reduce tasks
	N              int      // n'th map task (assigned number)
	SourceHosts    []string // address of the map workers sourcing input
	SourcePorts    []string // port of the map workers sourcing input
	Distributed    bool
	Finished       bool
	FinishedBy     string
	FinishedByPort string
}

type Pair struct {
	Key   string
	Value string
}

type MapLog struct {
	tasks int
	pairs int
}

type ReduceLog struct {
	keys   int
	values int
	pairs  int
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string     { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string      { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string  { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string   { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string  { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string    { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeTempDir(port string) string { return fmt.Sprintf("tmp%s/", port) }
func makeURL(host, port, file string) string {
	return fmt.Sprintf("http://%s/data/%s%s", host, makeTempDir(port), file)
}

func (task *MapTask) Process(tempdir string, client Interface) error {

	// download the input file
	if err := download(makeURL(task.SourceHost, task.SourcePort, mapSourceFile(task.N)), tempdir+mapInputFile(task.N)); err != nil {
		log.Printf("error downloading in MapTask.Process: %v", err)
	}

	// open the input file
	inputDB, err := openDatabase("data/" + tempdir + mapInputFile(task.N))
	if err != nil {
		log.Printf("error opening input file in MapTask.Process: %v", err)
	}

	// create the output files
	var outputDBs []*sql.DB
	for i := 0; i < task.R; i++ {
		newDB, err := createDatabase("data/" + tempdir + mapOutputFile(task.N, i))
		if err != nil {
			log.Printf("error creating output files in MapTask.Process: %v", err)
			return err
		}
		outputDBs = append(outputDBs, newDB)
	}

	// prepare statements
	var outputStmts []*sql.Stmt
	for _, db := range outputDBs {
		stmt, err := db.Prepare("insert into pairs (key, value) values (?, ?)")
		if err != nil {
			log.Fatalf("error in MapProcess statement generation: %v", err)
		}
		outputStmts = append(outputStmts, stmt)
		defer stmt.Close()
	}

	// run a query to select ALL PAIRS from SOURCE DB
	rows, err := inputDB.Query(`SELECT key, value FROM pairs`)
	if err != nil {
		log.Printf("error in MapTask.Process during query: %v", err)
		return err
	}
	var key, value string
	var mlog MapLog = MapLog{tasks: 0, pairs: 0}
	for rows.Next() {
		if err := rows.Scan(&key, &value); err != nil {
			log.Printf("error in splitDatabase during scan: %v", err)
			return err
		}

		// call client.Map with the pair AND launch go routine to collect output pair
		outputPair := make(chan Pair, 100)
		finished := make(chan struct{})

		go mapCollectPair(outputPair, finished, outputStmts, task.R, &mlog)

		if err := client.Map(key, value, outputPair); err != nil {
			log.Printf("error in MapTask.Process during client.Map: %v", err)
			return err
		}

		// accept a value from finished chan signaling 'sync'
		<-finished
	}

	// close databases before finishing the function
	inputDB.Close()
	for _, db := range outputDBs {
		db.Close()
	}

	fmt.Printf("map task processed %d pairs, generated %d pairs\n", mlog.tasks, mlog.pairs)
	return nil
}

func mapCollectPair(outputPair <-chan Pair, finished chan<- struct{}, outputStmts []*sql.Stmt, reduceTasks int, mlog *MapLog) {
	mlog.tasks += 1
	for pair := range outputPair {
		hash := fnv.New32()
		hash.Write([]byte(pair.Key))
		r := int(hash.Sum32() % uint32(reduceTasks))
		stmt := outputStmts[r]
		_, err := stmt.Exec(pair.Key, pair.Value)
		if err != nil {
			log.Fatalf("error in mapCollectPair during insert: %v", err)
			finished <- struct{}{}
		}
		mlog.pairs += 1
	}
	finished <- struct{}{}
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// create inputDB by merging map outputs
	
	var outputURLs []string
	for i, host := range task.SourceHosts {
		outputURLs = append(outputURLs, makeURL(host, task.SourcePorts[i], (mapOutputFile(i, task.N))))
	}

	inputDB, err := mergeDatabases(outputURLs, tempdir+reduceOutputFile(task.N), tempdir+reduceTempFile(task.N))
	if err != nil {
		log.Printf("error in ReduceTask.Process mergeDatabases: %v", err)
		return err
	}

	// create outputDB
	outputDB, err := createDatabase("data/" + tempdir + reduceOutputFile(task.N))
	if err != nil {
		log.Printf("error in ReduceTask.Process creating outputDB: %v", err)
		return err
	}

	// query pairs in correct order
	rows, err := inputDB.Query(`SELECT key, value FROM pairs ORDER BY key, value`)
	if err != nil {
		log.Printf("error in ReduceTask.Process during query: %v", err)
		return err
	}

	// iteration over query'd values
	var key, value, prevKey string
	var newKey bool = true
	var outputChan (chan Pair)
	var valChan (chan string)
	var finished (chan struct{})
	prevKey = ""
	rlog := ReduceLog{keys: 0, values: 0, pairs: 0}
	for rows.Next() {
		if err := rows.Scan(&key, &value); err != nil {
			log.Printf("error in ReduceTask.Process during scan: %v", err)
			close(finished)
			close(valChan)
			close(outputChan)
			return err
		}

		// is it a new key? (not including the first)
		if prevKey != "" && prevKey != key {
			newKey = true
			close(valChan)
			<-finished
		}

		// new key case
		if newKey {
			valChan = make(chan string)
			outputChan = make(chan Pair, 100)
			finished = make(chan struct{})

			go reduceCollectPair(outputChan, finished, outputDB, &rlog)
			go client.Reduce(key, valChan, outputChan)
		}

		valChan <- value
		prevKey = key
		newKey = false
	}

	// close all DBs before returning
	close(valChan)
	<-finished

	fmt.Printf("reduce task processed %d keys and %d values, generated %d pairs\n", rlog.keys, rlog.values, rlog.pairs)
	return nil
}

func reduceCollectPair(outputPair <-chan Pair, finished chan<- struct{}, outputDB *sql.DB, rlog *ReduceLog) {
	defer close(finished)
	stmt, err := outputDB.Prepare("insert into pairs (key, value) values (?, ?)")
	if err != nil {
		log.Fatalf("error in reduceCollectPair")
	}
	defer stmt.Close()

	rlog.keys += 1
	for pair := range outputPair {
		_, err := stmt.Exec(pair.Key, pair.Value)
		if err != nil {
			log.Fatalf("error in reduceCollectPair during insert: %v", err)
			finished <- struct{}{}
		}
		rlog.pairs += 1
		if i, err := strconv.Atoi(pair.Value); err != nil {
			log.Printf("error in reduceCollectPair: %v", err)
		} else {
			rlog.values += i
		}
	}
	finished <- struct{}{}
}

// UP TO query HAS BEEN COMPLETED/TESTED MINIMALLY

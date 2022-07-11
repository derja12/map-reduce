package mapreduce

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type handler func(*Tasks)

// Server channel is where the actor recieves functions to run
type Server chan handler

// Nothing is an empty struct for RPC purposes
type Nothing struct{}



// expects url => full http name [ex. http://localhost:8080/data/test.db],
// path => full path name of new file [ex. tmp/test.db]
func download(url, path string) error {
	// issue get request
	// fmt.Printf("path: %v url: %v\n", path, url)
	res, err := http.Get(url)
	if err != nil {
		log.Printf("error in download get request: %v", err)
		return err
	}
	// create file
	newFile, err := os.Create("data/" + path)
	if err != nil {
		log.Printf("error in download file creation: %v", err)
		return err
	}
	// copy from get response to file
	if _, err := io.Copy(newFile, res.Body); err != nil {
		log.Printf("error in download copying file: %v", err)
		return err
	}

	err = res.Body.Close()
	if err != nil {
		log.Printf("error in download closing body: %v", err)
		return err
	}

	return nil
}

func gatherInto(db *sql.DB, path string) error {
	// attach (open second database and attach)
	_, err := db.Exec("attach ? as merge;", "data/"+path)
	if err != nil {
		log.Printf("error in gatherInto attach: %v", err)
		return err
	}

	// merge (insert from the attached database 'merge')
	_, err = db.Exec("insert into pairs select * from merge.pairs;")
	if err != nil {
		log.Printf("error in gatherInto insert: %v", err)
		return err
	}

	// detach (unlinks the temporary database 'merge')
	_, err = db.Exec("detach merge;")
	if err != nil {
		log.Printf("error in gatherInto detach: %v", err)
		return err
	}

	return nil
}

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

func getInput(isMaster *bool, M, R *int, port, masterAddress, tempDir *string) {
	// get isMaster
	var err error
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Master Server? (y/n)\n")
	scanner.Scan()
	line := scanner.Text()
	line = strings.TrimSpace(line)
	if line == "y" {
		*isMaster = true
	}
	// if isMaster, get M and R
	if *isMaster {
		fmt.Printf("Number of Maptasks?\n")
		scanner.Scan()
		line = scanner.Text()
		line = strings.TrimSpace(line)
		if *M, err = strconv.Atoi(line); err != nil {
			log.Fatalf("error parsing MapTasks during startup: %v", err)
		}
		fmt.Printf("Number of Reducetasks?\n")
		scanner.Scan()
		line = scanner.Text()
		line = strings.TrimSpace(line)
		if *R, err = strconv.Atoi(line); err != nil {
			log.Fatalf("error parsing MapTasks during startup: %v", err)
		}
	}
	// get port
	fmt.Printf("Port?\n")
	scanner.Scan()
	line = scanner.Text()
	line = strings.TrimSpace(line)
	*port = line
	// get master address
	if !*isMaster {
		fmt.Printf("Master address?\n")
		scanner.Scan()
		line = scanner.Text()
		line = strings.TrimSpace(line)
		*masterAddress = line
	}
	// get tempDir
	*tempDir = fmt.Sprintf("tmp%s/", *port)
}

// shorten master code up a bit
func splitInputFile(m int, filename string, tempDir string) error {
	_, err := splitDatabase(filename, tempDir+"map_%d_source.db", m)
	if err != nil {
		log.Printf("error splitting in main: %v\n", err)
		return err
	}
	return nil
}

// helpful for debugging/knowing what's going on
func printTasks(tasks *Tasks) {
	for i := range tasks.MTasks {
		fmt.Printf("Map: M-%v, R-%v, N-%v, SourceHost-%v\n", tasks.MTasks[i].M, tasks.MTasks[i].R, tasks.MTasks[i].N, tasks.MTasks[i].SourceHost)
	}
	for i := range tasks.RTasks {
		fmt.Printf("Reduce: M-%v, R-%v, N-%v, SourceHosts-%v\n", tasks.RTasks[i].M, tasks.RTasks[i].R, tasks.RTasks[i].N, tasks.RTasks[i].SourceHosts)
	}
}

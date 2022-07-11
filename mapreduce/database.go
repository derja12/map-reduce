package mapreduce

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func openDatabase(path string) (*sql.DB, error) {
	// path := "somefile.db"
	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		log.Printf("error in openDatabase: %v\n", err)
	}
	return db, err
}

// path [ex. tmp/test.db]
func createDatabase(path string) (*sql.DB, error) {
	// remove the file if it exists
	// fmt.Printf("path: %v\n", path)
	os.Remove(path)

	// create sqlite file [ ]
	db, err := openDatabase(path)

	if err != nil {
		log.Printf("error opening database: %v\n", err)
	}

	_, err = db.Exec("create table pairs (key text, value text);")

	if err != nil {
		log.Printf("error executing create table pairs command: %v\n", err)
	}

	if err != nil {
		db.Close()
	}

	return db, err
}

// OUTPUT NAMES DOES NOT CONTAIN 'tmp/'
func splitDatabase(inputPath, outputPattern string, m int) ([]string, error) {
	var inputDB *sql.DB
	var outputDBs []*sql.DB
	var outputNames []string

	// open input database
	inputDB, err := openDatabase(inputPath)
	if err != nil {
		log.Fatalf("error in splitDatabase opening input database: %v\n", err)
	}

	// create pointers and names for output databases
	for i := 0; i < m; i++ {
		outputName := fmt.Sprintf(outputPattern, i)
		outputNames = append(outputNames, strings.TrimPrefix(outputName, "data/tmp/"))
		db, err := createDatabase(outputName)
		outputDBs = append(outputDBs, db)
		if err != nil {
			log.Fatalf("error in splitDatabase opening output databases: %v\n", err, outputName)
		}
	}

	// runs input query and iterate over outputs inserting rows
	var key, value string
	databaseIndex := 0
	keysProcessed := 0
	rows, err := inputDB.Query(`SELECT key, value FROM pairs`)
	if err != nil {
		log.Fatalf("error in splitDatabase during query: %v", err)
	}
	for rows.Next() {
		if err := rows.Scan(&key, &value); err != nil {
			log.Fatalf("error in splitDatabase during scan: %v", err)
		}
		databaseIndex %= m
		db := outputDBs[databaseIndex]
		_, err := db.Exec("insert into pairs (key, value) values (?, ?)", key, value)
		if err != nil {
			log.Fatalf("error in splitDatabase during insert: %v", err)
		}
		databaseIndex++
		keysProcessed++
	}

	// close all databases
	for _, db := range outputDBs {
		db.Close()
	}
	inputDB.Close()

	// final keys-processed check
	var errCheck error
	if keysProcessed < m {
		errCheck = errors.New(fmt.Sprintf("Not enough key-values processed: %v is less than expected %v or more", keysProcessed, m))
	}

	return outputNames, errCheck
}

// provide []string of COMPLETE urls [ex. http://localhost:8080/data/test.db],
// path for output database [ex. tmp/mergedAusten.db],
// temp string with full path [ex. tmp/test.db]
func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	// create output database

	fmt.Printf("\n\n")
	
	outputDB, err := createDatabase("data/" + path)
	if err != nil {
		log.Printf("error in mergeDatabase calling createDatabase: %v", err)
		return outputDB, err
	}

	// iteration over each url
	for _, url := range urls {
		// download into temp file
		if err := download(url, temp); err != nil {
			log.Printf("error in mergeDatabase calling download: %v", err)
			return outputDB, err
		}

		// merge into outputDB
		if err := gatherInto(outputDB, temp); err != nil {
			log.Printf("error in mergeDatabase calling gatherInto: %v", err)
			return outputDB, err
		}

		os.Remove(temp)
	}

	return outputDB, nil
}

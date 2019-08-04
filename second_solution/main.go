package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"json"
)

var fileName = flag.String("file", "", "Name of the file that has json data\nShould be in the same location as this binary")
var clearCache = flag.Bool("c", false, "Clear the cache")
var records = flag.Int("r", 100, "Number of records to read before caching")

func main() {
	done := make(chan struct{}, 1)
	defer func() {
		done <- struct{}{}
	}()

	flag.Parse()
	// Clear the cache via -c
	if *clearCache {
		if err := os.Remove(filepath.Join(filepath.Dir(*fileName), ".cache")); err != nil {
			panic(err)
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dir)
	absFilePath, err := filepath.Abs(*fileName)
	if err != nil {
		panic(err)
	}
	json.Process(*records, absFilePath)
}

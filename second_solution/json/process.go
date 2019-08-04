package json

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type row struct {
	UserID    string `json:"user_id"`
	TimeStamp int64
	Type      string
	EventName string `json:"name"`
	Data      map[string]interface{}
	Id        string
}

type stat struct {
	Events     map[string]int         // event_name as key , count
	Attributes map[string]interface{} // attribute_name
	Time       int64
}

//  Sort the event keys and attribute keys
func (s *stat) String() string {
	out := []string{}
	for _, k := range sortAttrKeys(s.Attributes) {
		out = append(out, fmt.Sprintf("%s=%s", k, s.Attributes[k]))
	}
	for _, k := range sortEventKeys(s.Events) {
		out = append(out, fmt.Sprintf("%s=%d", k, s.Events[k]))
	}
	return strings.Join(out, ",") + "\n"
}

func sortAttrKeys(m map[string]interface{}) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func sortEventKeys(m map[string]int) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Process processes the given data file.
// The output is saved in out.log. Any previously cached results will be read from '.cache'
func Process(recordsToCache int, fileName string) {
	path := filepath.Dir(fileName)
	users := map[int32]*stat{}
	records, seekTo := readCache(users, path)

	// read every line from the file and pass it to the channels
	rowC, eC, doneC := readData(seekTo, fileName)

	// get all the parsed users and their statistics
	processRows(recordsToCache, records, users, rowC, eC, doneC, path)

	var userIds []int32
	for u := range users {
		userIds = append(userIds, u)
	}
	sort.SliceStable(userIds, func(i, j int) bool { return userIds[i] < userIds[j] })

	writeResultToFile(userIds, users, path)
}

func convertToInt32(s string) int32 {
	if s == "" {
		return -1
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		panic(fmt.Errorf("error converting %q to int %v", s, err))
	}
	return int32(n)
}

type rowAndPosition struct {
	r            row
	filePosition int64
}

func readData(seekTo int64, fileName string) (<-chan rowAndPosition, <-chan error, <-chan struct{}) {
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	// Start reading the data file from the provided filePosition, which is read from the cache.
	_, err = file.Seek(seekTo, 0)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	// channel to send the parsed json object and the current file position.
	rowC := make(chan rowAndPosition)
	errC := make(chan error)
	doneC := make(chan struct{})
	var totalBytes int64
	totalBytes = seekTo
	go func() {
		defer file.Close()

		for scanner.Scan() {
			contents := scanner.Bytes()
			// +1 to accomodate the newline character.
			totalBytes = totalBytes + int64(len(contents)+1)
			if err != nil {
				errC <- fmt.Errorf("can't get current position from file %v ", err)
			}
			var r row
			if err := json.Unmarshal(contents, &r); err != nil {
				errC <- fmt.Errorf("unable to decode the json file contents:%q, err: %v,", contents, err)
			}
			rowC <- rowAndPosition{filePosition: totalBytes, r: r} //
		}
		doneC <- struct{}{}
		close(rowC)
		close(errC)
		close(doneC)
	}()
	return rowC, errC, doneC
}

func processRows(recordsToCache int, records int64, users map[int32]*stat, rowC <-chan rowAndPosition, errC <-chan error, doneC <-chan struct{}, path string) {
	uniqueIDs := map[string]bool{}
	var filePosition int64
	totalRows := records
	for {
		select {
		case row := <-rowC:
			totalRows++
			r := row.r
			filePosition = row.filePosition
			userId := convertToInt32(r.UserID)
			if userId == -1 {
				break
			}
			if r.Type == "event" {
				if u, ok := users[userId]; ok {
					if !uniqueIDs[r.Id] {
						if u.Events == nil {
							u.Events = make(map[string]int)
						}
						u.Events[r.EventName]++
						uniqueIDs[r.Id] = true
					}
					break
				}
				users[userId] = &stat{
					Attributes: make(map[string]interface{}),
					Events:     map[string]int{r.EventName: 1},
				}
			}
			if r.Type == "attributes" {
				if u, ok := users[userId]; ok {
					if u.Time < r.TimeStamp {
						u.Attributes = r.Data
						u.Time = r.TimeStamp
					}
					break
				}
				users[userId] = &stat{
					Time:       r.TimeStamp,
					Attributes: r.Data,
					Events:     make(map[string]int),
				}
			}
		case err := <-errC:
			if err != nil {
				panic(err)
			}
		case <-doneC:
			return
		}
		if totalRows%int64(recordsToCache) == 0 {
			fmt.Printf("records processed %d, users processed %d\n", totalRows, len(users))
			writeToCache(totalRows, filePosition, users, path)
		}
	}
}

// readCache reads the cache file '.cache' and returns the file position to seek to.
func readCache(users map[int32]*stat, path string) (int64, int64) {
	cacheFilePath := filepath.Join(path, ".cache")
	file, err := os.Open(cacheFilePath)
	defer file.Close()
	if os.IsNotExist(err) {
		file, err = os.OpenFile(cacheFilePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		return 0, 0
	}
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(file)
	line, _, err := reader.ReadLine()
	if err == io.EOF {
		// cache is empty
		return 0, 0
	}
	if err != nil {
		panic(err)
	}
	// convert the records from string to int64.
	records, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		panic(err)
	}

	line, _, err = reader.ReadLine()
	if err != nil {
		panic(err)
	}
	// convert the file position from string to int64.
	filePosition, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		panic(err)
	}
	bytes := make([]byte, 1024)
	// total will hold all the rest of the cached contents of the file.
	total := []byte{}
	n, err := reader.Read(bytes)
	// Keep reading util reach end of file.
	for err != io.EOF && n > 0 {
		// copy only bytes that have been read from the file.
		total = append(total, bytes[0:n]...)
		n, err = reader.Read(bytes)
	}
	if err != nil && err != io.EOF {
		panic(err)
	}
	// Unmarshal the cached json data into map[int32]*stat.
	if err := json.Unmarshal(total, &users); err != nil {
		panic(err)
	}
	return records, filePosition
}

func writeToCache(records int64, filePosition int64, users map[int32]*stat, path string) {
	cacheFilePath := filepath.Join(path, ".cache")
	f, err := os.OpenFile(cacheFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	bytesToWrite := []byte(fmt.Sprintf("%d", records))
	bytesToWrite = append(bytesToWrite, []byte("\n")...)

	bytesToWrite = append(bytesToWrite, []byte(fmt.Sprintf("%d", filePosition))...)
	bytesToWrite = append(bytesToWrite, []byte("\n")...)

	bytes, err := json.MarshalIndent(users, " ", " ")
	if err != nil {
		panic(err)
	}
	bytesToWrite = append(bytesToWrite, bytes...)
	if _, err = f.Write(bytesToWrite); err != nil {
		panic(err)
	}
}

func writeResultToFile(userIds []int32, users map[int32]*stat, path string) {
	outFile := filepath.Join(path, "out.log")
	os.Remove(outFile)
	f, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	for _, u := range userIds {
		if _, err := fmt.Fprintf(f, "%d,%s", u, users[u]); err != nil {
			panic(err)
		}

	}
}

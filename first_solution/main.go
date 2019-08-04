package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

var fileName = flag.String("file", "", "Name of the file that has json data\nShould be in the same location as this binary")

type Record struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Name      string            `json:"name"`
	UserID    string            `json:"user_id"`
	Data      map[string]string `json:"data"`
	Timestamp int64             `json:"timestamp"`
}

// Attribute struct
type Attribute struct {
	Timestamp int64
	Value     string
}

// Event struct
type Event struct {
	IDs   []string
	Count int
}

// Stat struct
type Stat struct {
	Attributes map[string]Attribute // attribute_name as key
	Events     map[string]Event     // eventname as key to count events
}

func main() {
	flag.Parse()

	// init the stats to store all the information
	stats := make(map[string]Stat)
	// Process the message
	stats = Processing(stats)
	// Sort all the userid and store
	userIds := sortUserId(stats)
	// Formatting the output
	writeFile(userIds, stats)

}

func Processing(stats map[string]Stat) map[string]Stat {
	// Open the data file
	lines, err := os.Open(*fileName)
	if err != nil {
		panic(err)

	}

	defer lines.Close()

	scanner := bufio.NewScanner(lines)

	for scanner.Scan() {
		contents := scanner.Bytes()
		var message Record
		if err := json.Unmarshal(contents, &message); err != nil {
			log.Fatal(err)
		} else {
			if message.UserID == "" || message.Timestamp < 0 {
				// empty ID

			}
			// Detect if the UserID exists, add it if not exists
			if _, hasKey := stats[message.UserID]; !hasKey {
				stats[message.UserID] = Stat{make(map[string]Attribute), make(map[string]Event)}
			}
			// append the value based on event or attribute
			switch dataType := message.Type; dataType {
			case "event":
				if event, exist := stats[message.UserID].Events[message.Name]; !exist {
					stats[message.UserID].Events[message.Name] = Event{[]string{message.ID}, 1}
				} else {
					// detect the duplicate ID, only count if not duplicated
					if d := duplicated(event.IDs, message.ID); !d {
						event.IDs = append(event.IDs, message.ID)
						event.Count++
						stats[message.UserID].Events[message.Name] = event

					}
				}
			case "attributes":
				for name, value := range message.Data {
					if attribute, exist := stats[message.UserID].Attributes[name]; !exist || message.Timestamp > attribute.Timestamp {
						stats[message.UserID].Attributes[name] = Attribute{message.Timestamp, value}

					}
				}
			}

		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return stats
}

// store all the userid and sort

func sortUserId(stats map[string]Stat) []int64 {
	var userIds []int64
	for u := range stats {
		n, err := strconv.ParseInt(u, 10, 64)
		if err != nil {
			continue
		}

		userIds = append(userIds, n)
	}
	sort.SliceStable(userIds, func(i, j int) bool { return userIds[i] < userIds[j] })
	return userIds
}

// Formating the output
func writeFile(userIds []int64, stats map[string]Stat) {
	output, err := os.Create("output.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()
	writer := bufio.NewWriter(output)

	for _, userid := range userIds {
		output_string := strconv.FormatInt(userid, 10)
		stat := stats[output_string]
		for attributeName, attribute := range stat.Attributes {
			output_string += fmt.Sprintf(",%s=%s", attributeName, attribute.Value)
		}
		for eventName, event := range stat.Events {
			output_string += fmt.Sprintf(",%s=%d", eventName, event.Count)
		}
		writer.WriteString(output_string + "\n")
	}
	writer.Flush()
	fmt.Printf("Done\n")

}

// detect if the ID exists
func duplicated(ids []string, id string) bool {
	for _, i := range ids {
		if i == id {
			return true
		}
	}
	return false
}

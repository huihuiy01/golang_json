# golang_json

# cd first_solution && go run main.go -file data.json

Read the file and loading into the object, then output to output.txt 

Dind't count the empty userid
Remove the duplicate ID for the same user 


# The second solution 

Please copy the process.go to your **GOPATH** (json/process.go)
go run main.go -file data.json -r 100000  ( -c to clear the cache when you need )



#My first idea is to record the position then write to the disk after processing some data 

/data/userid/events | attributes  

events/
eventname -> record the count 


attributes/

attribute/ { timestamp1, 2,3) 


## New Design


Then I found this didn't work well due to too many open files. I'm not familary with the how does golang handle the file.Close(). 


After research and consideration, I redesign to use struct data to store all the cache data for loading. 

create channel to send data for processing line by line
write cache to the disk after process amount of lines, based on the testing, no pressue to handle 10,000 lines





package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/Clever/mongo-to-s3/aws"
	"github.com/Clever/mongo-to-s3/config"
	"github.com/Clever/mongo-to-s3/fab"

	"github.com/Clever/pathio"
	"gopkg.in/Clever/optimus.v3"
	json "gopkg.in/Clever/optimus.v3/sinks/json"
	mongosource "gopkg.in/Clever/optimus.v3/sources/mongo"
	"gopkg.in/Clever/optimus.v3/transformer"
	"gopkg.in/mgo.v2"
)

var (
	configPath = flag.String("config", "config.yml", "Path to config file (default: config.yml)")
	url        = flag.String("database", "", "Database url if using existing instance")
	s3         = flag.String("s3", "", "s3 url to upload to (default: none)")
)

// Running instance using fab takes up to ~10 minutes, so will retry over this time period, then fail after 10 minutes
func mongoConnection(url string) *mgo.Session {
	s, err := mgo.DialWithTimeout(url, 10*time.Minute)
	if err != nil {
		log.Fatal("err connecting to mongo instance: ", err)
	}
	s.SetMode(mgo.Monotonic, true)
	return s
}

// parseConfigFile loads the config from wherever it is, then parses it
func parseConfigFile(path string) config.Config {
	reader, err := pathio.Reader(path)
	defer reader.Close()
	if err != nil {
		log.Fatal("err opening config file: ", err)
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal("err reading file: ", err)
	}
	config, err := config.ParseYAML(data)
	if err != nil {
		log.Fatal("err parsing config file: ", err)
	}

	return config
}

func configuredOptimusTable(s *mgo.Session, table config.Table) optimus.Table {
	collection := s.DB("").C(table.Source)
	selector := table.MongoSelector()
	iter := collection.Find(nil).Select(selector).Iter()
	return mongosource.New(iter)
}

func createOutputFile(timestamp, collectionName, extension string) *os.File {
	name := fmt.Sprintf("%v_mongo_%v%v", timestamp, collectionName, extension)
	file, err := os.Create(name)
	if err != nil {
		log.Fatal("err creating output file: ", err)
	}
	return file
}

func exportData(source optimus.Table, table config.Table, sink optimus.Sink) (int, error) {
	rows := 0
	err := transformer.New(source).Fieldmap(table.FieldMap()).Map(
		func(d optimus.Row) (optimus.Row, error) {
			rows = rows + 1
			return optimus.Row(d), nil
		}).Sink(sink)
	return rows, err
}

func copyConfigFile(timestamp, path string) {
	input, err := pathio.Reader(path)
	if err != nil {
		log.Fatal("error opening config file", err)
	}
	output := createOutputFile(timestamp, "config", ".yml")
	if err != nil {
		log.Fatal("error creating config file", err)
	}
	_, err = io.Copy(output, input)
	if err != nil {
		log.Fatal("error writing output file: ", err)
	}
}

func main() {
	flag.Parse()

	var instance fab.Instance
	if *url == "" {
		instance, err := fab.CreateSISDBFromLatestSnapshot("analytics-test")
		if err != nil {
			log.Fatal("err starting db: ", err)
		}
		*url = instance.URL
	}
	fmt.Println("url : ", *url)
	s := mongoConnection(*url)
	log.Println("Connected to mongo")

	c := aws.NewClient("us-west-1")
	// Times are rounded down to the nearest hour
	timestamp := time.Now().Add(-1 * time.Hour / 2).Round(time.Hour).Format(time.RFC3339)
	if instance.SnapshotID != "" {
		snapshot, err := c.FindSnapshot(instance.SnapshotID)
		if err != nil {
			log.Println("err finding latest snapshot: ", err)
		} else {
			timestamp = snapshot.StartTime.Add(-1 * time.Hour / 2).Round(time.Hour).Format(time.RFC3339)
		}
	}

	config := parseConfigFile(*configPath)
	copyConfigFile(timestamp, *configPath)
	for _, table := range config {
		output := createOutputFile(timestamp, table.Destination, ".json.gz")
		defer output.Close()

		// Gzip output to the file
		zippedOutput := gzip.NewWriter(output) // sorcery
		defer zippedOutput.Close()
		sink := json.New(zippedOutput)

		source := configuredOptimusTable(s, table)
		count, err := exportData(source, table, sink)
		if err != nil {
			log.Fatal("err reading table: ", err)
		}
		log.Println(table.Destination, " collection: ", count, " items")

		// Upload file to bucket
		if *s3 != "" {
			if _, err := output.Seek(0, 0); err != nil {
				log.Fatal("err reading output for upload: ", err)
			}
			if err := pathio.WriteReader(*s3, output); err != nil {
				log.Fatal("err uploading to s3 bucket: ", err)
			}
		}
	}

	if instance.ID != "" {
		log.Println("terminating instance")
		err := c.TerminateInstance(instance.ID)
		if err != nil {
			log.Println("err terminating instance: ", err)
		}
	}
}

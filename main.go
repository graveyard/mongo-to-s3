package main

import (
	"compress/gzip"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/Clever/mongo-to-s3/aws"
	"github.com/Clever/mongo-to-s3/config"

	"github.com/Clever/pathio"
	"gopkg.in/Clever/optimus.v3"
	json "gopkg.in/Clever/optimus.v3/sinks/json"
	mongosource "gopkg.in/Clever/optimus.v3/sources/mongo"
	"gopkg.in/Clever/optimus.v3/transformer"
	"gopkg.in/mgo.v2"
)

const (
	MONGO_URL = "MONGO_URL"
)

var (
	configPath   = flag.String("config", "config.yml", "Path to config file")
	databaseName = flag.String("database", "clever", "Database url connects to (default: Clever)")
	s3           = flag.String("s3", "", "s3 url to upload to (default: none)")
)

func main() {
	flag.Parse()

	s := MongoConnection()
	log.Println("Connected to mongo")

	config := ParseConfigFile()
	for _, table := range config {
		if table.Source != "districts" {
			continue // DEBUG
		}

		// Create the output file
		output, err := CreateOutputFile(table.Destination, ".json.gz")
		if err != nil {
			log.Fatal("err creating output file: ", err)
		}
		defer output.Close()

		// Gzip output to the file
		zippedOutput := gzip.NewWriter(output) // sorcery
		defer zippedOutput.Close()
		sink := json.New(zippedOutput)

		iter := ConfiguredIterator(s, table)

		count, err := ExportData(mongosource.New(iter), table, sink)
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
}

func MongoConnection() *mgo.Session {
	mongo := os.Getenv(MONGO_URL)
	if mongo == "" {
		log.Fatal("missing environment MONGO_URL")
	}
	s, err := mgo.Dial(mongo)
	if err != nil {
		log.Fatal("foo err: ", err)
	}
	s.SetMode(mgo.Monotonic, true)

	return s
}

func ParseConfigFile() config.Config {
	data, err := ioutil.ReadFile(*configPath)
	if err != nil {
		log.Fatal("err opening config file: ", err)
	}
	config, err := config.ParseYAML(data)
	if err != nil {
		log.Fatal("err parsing config file: ", err)
	}

	return config
}

func ConfiguredIterator(s *mgo.Session, table config.Table) *mgo.Iter {
	collection := s.DB(*databaseName).C(table.Source)
	selector := table.MongoSelector()
	return collection.Find(nil).Select(selector).Iter()
}

func CreateOutputFile(collectionName, extension string) (*os.File, error) {
	name := time.Now().Format("2006-01-02T15:04:05MST") + "_mongo_" + collectionName + extension
	return os.Create(name)
}

func ExportData(source optimus.Table, table config.Table, sink optimus.Sink) (int, error) {
	rows := 0
	err := transformer.New(source).Fieldmap(table.FieldMap()).Map(
		func(d optimus.Row) (optimus.Row, error) {
			rows = rows + 1
			return optimus.Row(d), nil
		}).Sink(sink)
	return rows, err
}

package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Clever/mongo-to-s3/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	json "github.com/pquerna/ffjson/ffjson"

	"github.com/Clever/analytics-util/analyticspipeline"
	"github.com/Clever/discovery-go"
	"github.com/Clever/pathio"
	"gopkg.in/Clever/optimus.v3"
	jsonsink "gopkg.in/Clever/optimus.v3/sinks/json"
	mongosource "gopkg.in/Clever/optimus.v3/sources/mongo"
	"gopkg.in/Clever/optimus.v3/transformer"
	"gopkg.in/Clever/optimus.v3/transforms"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var configs map[string]string

// getEnv looks up an environment variable given and exits if it does not exist.
func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		log.Fatalf("Must specify env variable %s", envVar)
	}
	return val
}

func generateServiceEndpoint(user, pass, path string) string {
	hostPort, err := discovery.HostPort("gearman-admin", "http")
	if err != nil {
		log.Fatal(err)
	}
	proto, err := discovery.Proto("gearman-admin", "http")
	if err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%s://%s:%s@%s%s", proto, user, pass, hostPort, path)
}

func init() {
	configs = map[string]string{
		"il":      getEnv("IL_CONFIG"),
		"sis":     getEnv("SIS_CONFIG"),
		"app_sis": getEnv("APP_SIS_CONFIG"),
		"legacy":  getEnv("LEGACY_CONFIG"),
	}
}

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
	configYaml, err := config.ParseYAML(data)
	if err != nil {
		log.Fatal("err parsing config file: ", err)
	}

	return configYaml
}

// parseConfigString takes in a config from an env var
func parseConfigString(conf string) config.Config {
	configYaml, err := config.ParseYAML([]byte(conf))
	if err != nil {
		log.Fatal("err parsing config file: ", err)
	}

	return configYaml
}

func configuredOptimusTable(s *mgo.Session, table config.Table) optimus.Table {
	fields := bson.M{}
	if table.Meta.UseProjectionOptimization == true {
		// Create a projection to only pull the fields we're interested in
		for _, f := range table.Fields {
			fields[f.Source] = 1
		}
	}

	collection := s.DB("").C(table.Source)
	iter := collection.Find(nil).Batch(1000).Prefetch(0.75).Select(fields).Iter()
	return mongosource.New(iter)
}

func formatFilename(timestamp, collectionName, fileIndex, extension string) string {
	if fileIndex != "" {
		// add underscore for readability
		fileIndex = fmt.Sprintf("_%s", fileIndex)
	}
	return fmt.Sprintf("mongo_%s_%s%s%s", collectionName, timestamp, fileIndex, extension)
}

func exportData(source optimus.Table, table config.Table, sink optimus.Sink, timestamp string) (int, error) {
	rows := 0
	datePopulator := config.GetPopulateDateFn(table.Meta.DataDateColumn, timestamp)
	existentialTransformer := config.GetExistentialTransformerFn(table)
	err := transformer.New(source).Map(config.Flattener()).
		Map(existentialTransformer). // convert PII to boolean exists or not
		Fieldmap(table.FieldMap()).
		Map(datePopulator). // add in the _data_timestamp, etc
		Map(func(d optimus.Row) (optimus.Row, error) {
			rows = rows + 1
			return d, nil
		}).Sink(sink)
	return rows, err
}

func copyConfigFile(bucket, timestamp, data, configName string) string {
	// config_name is parsed from the input path b/c we have a different configs`
	// get the yaml file at the end of the path
	outPath := formatFilename(timestamp, configName, "", ".yml")
	if bucket != "" {
		outPath = fmt.Sprintf("s3://%s/%s", bucket, outPath)
	}
	log.Printf("uploading conf file to: %s", outPath)
	err := pathio.Write(outPath, []byte(data))
	if err != nil {
		log.Fatal("error writing output file: ", err)
	}
	return outPath
}

// Given the command line inputs and the config file, choose the tables we want to push to s3
func getTableFromConf(sourceInput string, configYaml config.Config) (config.Table, error) {
	// none specified, throw error
	if sourceInput == "" {
		log.Println("no collection specified, throwing error")
		return config.Table{}, fmt.Errorf("No collection specified")
	}
	// collection was specified, get the right one
	log.Printf("fetching collection specified: %s", sourceInput)
	curTable := config.Table{}
	for _, table := range configYaml {
		if sourceInput == table.Source {
			curTable = table
		}
	}
	// if not set, yell
	if curTable.Destination == "" {
		return config.Table{}, fmt.Errorf("Could not find source table: %s in config", sourceInput)
	}
	return curTable, nil
}

// uploadFile handles the awkwardness around s3 regions to upload the file
// it takes in a reader for maximum flexibility
func uploadFile(reader io.Reader, bucket, outputName string) {
	s3Path := fmt.Sprintf("s3://%s/%s", bucket, outputName)
	log.Printf("uploading file: %s to path: %s", outputName, s3Path)
	region, err := getRegionForBucket(bucket)
	if err != nil {
		log.Fatalf("err getting region for bucket: %s", err)
	}
	log.Printf("found bucket region: %s", region)

	// required to do this since we can't pipe together the gzip output and pathio, unfortunately
	// TODO: modify Pathio so that we can support io.Pipe and use Pathio here: https://clever.atlassian.net/browse/IP-353
	// from https://github.com/aws/aws-sdk-go/wiki/Getting-Started-Common-Examples
	session := session.New()
	client := s3.New(session, aws.NewConfig().WithRegion(region))
	uploader := s3manager.NewUploaderWithClient(client)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:                 reader,
		Bucket:               aws.String(bucket),
		Key:                  aws.String(outputName),
		ServerSideEncryption: aws.String("AES256"),
	})
	if err != nil {
		log.Fatalf("err uploading to s3 path: %s, err: %s", s3Path, err)
	}
}

// EntryArray is a convenience function for JSON marshalling
type EntryArray []map[string]interface{}

// Manifest represents an s3 manifest file used to download to redshift
// really only useful for JSON marshalling
type Manifest struct {
	Entries EntryArray `json:"entries"`
}

// createManifest creates a manifest file given the list of files to include into the file
// it returns a reader for convenience
// looks something like:
//  { "entries": [
//    {"url": "s3://clever-analytics/mongo_students_1_2016-01-27T21:00:00Z.json.gz", "mandatory": true},
//    {"url": "s3://clever-analytics/mongo_students_2_2016-01-27T21:00:00Z.json.gz", "mandatory": true}
//  ] }
func createManifest(bucket string, dataFilenames []string) (io.Reader, error) {
	var entryArray EntryArray
	for _, fn := range dataFilenames {
		entryArray = append(entryArray, map[string]interface{}{
			"url":       fmt.Sprintf("s3://%s/%s", bucket, fn),
			"mandatory": true,
		})
	}

	jsonVal, err := json.Marshal(Manifest{Entries: entryArray})
	if err != nil {
		return nil, err
	}
	log.Printf("Manifest file contents: %s", string(jsonVal))
	return bytes.NewReader(jsonVal), nil
}

func main() {
	flags := struct {
		Name       string `config:"config"`
		Collection string `config:"collection"`
		URL        string `config:"database"`
		Bucket     string `config:"bucket"`
		NumFiles   string `config:"numfiles"` // configure library doesn't support ints or floats
	}{ // specifying default values:
		Name:       "",
		Collection: "",
		URL:        "",
		Bucket:     "TODO",
		NumFiles:   "1",
	}

	nextPayload, err := analyticspipeline.AnalyticsWorker(&flags)
	if err != nil {
		log.Fatalf("err: %#v", err)
	}

	numFiles, err := strconv.Atoi(flags.NumFiles)
	if err != nil {
		log.Fatal(err)
	}
	if numFiles < 1 {
		log.Fatal("Must specify a number of output file parts >= 1")
	}
	if flags.URL == "" {
		log.Fatal("Database url of existing instance is necessary")
	}

	mongoClient := mongoConnection(flags.URL)
	log.Println("Connected to mongo")

	// Times are rounded down to the nearest hour
	timestamp := time.Now().UTC().Add(-1 * time.Hour / 2).Round(time.Hour).Format(time.RFC3339)

	//configYaml := parseConfigFile(flags.ConfigPath)

	c, ok := configs[flags.Name]
	if !ok {
		log.Fatal("config sucks")
	}
	configYaml := parseConfigString(c)
	confFileName := copyConfigFile(flags.Bucket, timestamp, c, flags.Name)
	sourceTable, err := getTableFromConf(flags.Collection, configYaml)
	if err != nil {
		log.Fatal(err)
	}

	// add name to list for submitting to next step in pipeline
	outputTableName := sourceTable.Destination
	outputFilenames := []string{}

	// verify total rows match sum of written
	var totalSummedRows int64
	var totalMongoRows int64

	mongoSource := configuredOptimusTable(mongoClient, sourceTable)
	mongoSource = optimus.Transform(mongoSource, transforms.Each(func(d optimus.Row) error {
		totalMongoRows++
		if totalMongoRows%1000000 == 0 {
			log.Printf("Processing mongo row: %d", totalMongoRows)
		}
		return nil
	}))

	// we want to split up the file for performance reasons
	var waitGroup sync.WaitGroup
	waitGroup.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		outputName := formatFilename(timestamp, sourceTable.Destination, strconv.Itoa(i), ".json.gz")
		outputFilenames = append(outputFilenames, outputName)
		log.Printf("Outputting file number: %d to location: %s", i, outputName)

		// Gzip output into pipe so that we don't need to store locally
		reader, writer := io.Pipe()
		go func(index int) {
			zippedOutput, _ := gzip.NewWriterLevel(writer, gzip.BestSpeed) // sorcery
			if err != nil {
				log.Fatal("invalid compression level: ", err)
			}

			sink := jsonsink.New(zippedOutput)
			// ALWAYS close the gzip first
			// (defer does LIFO)
			defer writer.Close()
			defer zippedOutput.Close()

			count, err := exportData(mongoSource, sourceTable, sink, timestamp)
			if err != nil {
				log.Fatal("err reading table: ", err)
			}
			log.Printf("Output destination collection: %s, count: %d, fileIndex: %d", sourceTable.Destination, count, index)
			// need to do this atomically to avoid concurrency issues
			atomic.AddInt64(&totalSummedRows, int64(count))
		}(i)

		// Upload file to bucket
		// need to put in own goroutine to kick off because exportData can't start and the reader can't close
		// until we hook up the reader to a sink via uploadFile
		// can't just put without goroutine because then only one iteration of the loop gets to run
		go func() {
			defer waitGroup.Done()
			uploadFile(reader, flags.Bucket, outputName)
		}()
	}
	waitGroup.Wait()
	log.Printf("Output %d total rows in %d files", totalSummedRows, numFiles)
	if totalSummedRows != totalMongoRows {
		log.Fatalf("number of rows written to s3: %d does not match the number of rows pulled from mongo: %d", totalMongoRows, totalSummedRows)
	}
	// we always upload a manifest including the files we just created
	manifestFilename := formatFilename(timestamp, sourceTable.Destination, "", ".manifest")
	manifestReader, err := createManifest(flags.Bucket, outputFilenames)
	if err != nil {
		log.Fatalf("Error creating manifest: %s", err)
	}
	uploadFile(manifestReader, flags.Bucket, manifestFilename)

	nextPayload.Current["tables"] = outputTableName
	nextPayload.Current["config"] = confFileName
	nextPayload.Current["date"] = timestamp

	analyticspipeline.PrintPayload(nextPayload)
}

// getRegionForBucket looks up the region name for the given bucket
func getRegionForBucket(name string) (string, error) {
	// Any region will work for the region lookup, but the request MUST use
	// PathStyle
	config := aws.NewConfig().WithRegion("us-west-1").WithS3ForcePathStyle(true)
	session := session.New()
	client := s3.New(session, config)
	params := s3.GetBucketLocationInput{
		Bucket: aws.String(name),
	}
	resp, err := client.GetBucketLocation(&params)
	if err != nil {
		return "", fmt.Errorf("Failed to get location for bucket '%s', %s", name, err)
	}
	if resp.LocationConstraint == nil {
		// "US Standard", returns an empty region. So return any region in the US
		return "us-east-1", nil
	}
	return *resp.LocationConstraint, nil
}

package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"net"
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
	"gopkg.in/Clever/kayvee-go.v6/logger"

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

var (
	log            = logger.New("mongo-to-s3")
	configs        map[string]string
	mongoURLs      map[string]string
	mongoUsernames map[string]string
	mongoPasswords map[string]string
)

// getEnv looks up an environment variable given and exits if it does not exist.
func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		log.ErrorD("env-variable-not-specified-error", logger.M{"variable": envVar})
		os.Exit(1)
	}
	return val
}

func generateServiceEndpoint(user, pass, path string) string {
	hostPort, err := discovery.HostPort("gearman-admin", "http")
	if err != nil {
		log.ErrorD("gearman-admin-discovery-host-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}
	proto, err := discovery.Proto("gearman-admin", "http")
	if err != nil {
		log.ErrorD("gearman-admin-discovery-proto-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}

	return fmt.Sprintf("%s://%s:%s@%s%s", proto, user, pass, hostPort, path)
}

func init() {
	configs = map[string]string{
		"il":           getEnv("IL_CONFIG"),
		"sis":          getEnv("SIS_CONFIG"),
		"sis_read":     getEnv("SIS_READ_CONFIG"),
		"app_sis":      getEnv("APP_SIS_CONFIG"),
		"app_sis_read": getEnv("APP_SIS_READ_CONFIG"),
		"legacy":       getEnv("LEGACY_CONFIG"),
		"legacy_read":  getEnv("LEGACY_READ_CONFIG"),
	}
	mongoURLs = map[string]string{
		"il":           getEnv("IL_URL"),
		"sis":          getEnv("SIS_URL"),
		"sis_read":     getEnv("SIS_READ_URL"),
		"app_sis":      getEnv("APP_SIS_URL"),
		"app_sis_read": getEnv("APP_SIS_READ_URL"),
		"legacy":       getEnv("LEGACY_URL"),
		"legacy_read":  getEnv("LEGACY_READ_URL"),
	}
	mongoUsernames = map[string]string{
		"il": getEnv("IL_USERNAME"),
	}
	mongoPasswords = map[string]string{
		"il": getEnv("IL_PASSWORD"),
	}
}

func mongoConnection(url string) *mgo.Session {
	s, err := mgo.DialWithTimeout(url, 10*time.Minute)
	if err != nil {
		log.ErrorD("mongo-dial-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}
	s.SetMode(mgo.Monotonic, true)
	return s
}

func mongoAtlasConnection(url string, username string, password string) (*mgo.Session, error) {
	log.InfoD("mongo-connection-call", logger.M{"url": url})
	dialInfo, err := mgo.ParseURL(url)
	if err != nil {
		log.ErrorD("mongo-parse-url-error", logger.M{"error": err.Error()})
		return nil, err
	}

	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		return tls.Dial("tcp", addr.String(), &tls.Config{})
	}
	if username != "" {
		log.Info("mongo-username-set")
		dialInfo.Username = username
		if password != "" {
			log.Info("mongo-password-set")
			dialInfo.Password = password
		}
	}

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.ErrorD("mongo-dial-error", logger.M{"error": err.Error()})
		return nil, err
	}
	log.Info("mongo-dial-successful")
	session.SetMode(mgo.Secondary, true)

	return session, nil
}

// parseConfigString takes in a config from an env var
func parseConfigString(conf string) config.Config {
	configYaml, err := config.ParseYAML([]byte(conf))
	if err != nil {
		log.ErrorD("config-parse-error", logger.M{"error": err.Error()})
		os.Exit(1)
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
	t, _ := time.Parse(time.RFC3339, timestamp)
	filePath := fmt.Sprintf("mongo/%s/_data_timestamp_year=%02d/_data_timestamp_month=%02d/_data_timestamp_day=%02d/",
		collectionName, t.Year(), int(t.Month()), t.Day())
	fileName := fmt.Sprintf("mongo_%s_%s%s%s", collectionName, timestamp, fileIndex, extension)
	return filePath + fileName
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
	log.InfoD("conf-file-upload", logger.M{"path": outPath})
	err := pathio.Write(outPath, []byte(data))
	if err != nil {
		log.ErrorD("output-file-write-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}
	return outPath
}

// uploadFile handles the awkwardness around s3 regions to upload the file
// it takes in a reader for maximum flexibility
func uploadFile(reader io.Reader, bucket, outputName string) {
	s3Path := fmt.Sprintf("s3://%s/%s", bucket, outputName)
	log.InfoD("uploading-file", logger.M{"filename": outputName, "path": s3Path})
	region, err := getRegionForBucket(bucket)
	if err != nil {
		log.ErrorD("bucket-region-retrieval-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}
	log.InfoD("bucket-region-found", logger.M{"region": region})

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
		log.ErrorD("s3-upload-error", logger.M{"path": s3Path, "error": err})
		os.Exit(1)
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
	log.InfoD("manifest-file-contents", logger.M{"value": string(jsonVal)})
	return bytes.NewReader(jsonVal), nil
}

func main() {
	flags := struct {
		Name       string `config:"config"`
		Collection string `config:"collection"`
		Bucket     string `config:"bucket"`
		NumFiles   string `config:"numfiles"` // configure library doesn't support ints or floats
	}{ // specifying default values:
		Name:       "",
		Collection: "",
		Bucket:     "TODO",
		NumFiles:   "1",
	}

	nextPayload, err := analyticspipeline.AnalyticsWorker(&flags)
	if err != nil {
		log.ErrorD("analyticspipeline-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}

	numFiles, err := strconv.Atoi(flags.NumFiles)
	if err != nil {
		log.ErrorD("num-files-atoi-error", logger.M{"error": err.Error()})
		os.Exit(1)
	}
	if numFiles < 1 {
		log.ErrorD("output-files-number-error", logger.M{"error": "Must specify a number of output file parts >= 1"})
		os.Exit(1)
	}

	// Times are rounded down to the nearest hour
	timestamp := time.Now().UTC().Add(-1 * time.Hour / 2).Round(time.Hour).Format(time.RFC3339)

	c, ok := configs[flags.Name]
	if !ok {
		log.Error("invalid-config-error")
		os.Exit(1)
	}
	configYaml := parseConfigString(c)
	confFileName := copyConfigFile(flags.Bucket, timestamp, c, flags.Name)

	if flags.Collection == "" {
		log.Error("no-collection-specified")
		os.Exit(1)
	}

	log.InfoD("collection-specified", logger.M{"collection": flags.Collection})

	sourceTable, ok := configYaml[flags.Collection]
	if !ok {
		log.ErrorD("config-table-not-found", logger.M{"key": flags.Collection})
		os.Exit(1)
	}

	mongoURL := mongoURLs[flags.Name]
	mongoUsername, ok := mongoUsernames[flags.Name]
	mongoPassword, ok := mongoPasswords[flags.Name]
	var mongoClient *mgo.Session
	if flags.Name == "il" {
		mongoClient, err = mongoAtlasConnection(mongoURL, mongoUsername, mongoPassword)
		if err != nil {
			log.ErrorD("mongo-connection-error", logger.M{"error": err.Error()})
			os.Exit(1)
		}
	} else {
		mongoClient = mongoConnection(mongoURL)
	}
	log.Info("mongo-connection-successful")

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
			log.InfoD("processing-mongo-row", logger.M{"numRows": totalMongoRows})
		}
		return nil
	}))

	// we want to split up the file for performance reasons
	var waitGroup sync.WaitGroup
	waitGroup.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		outputName := formatFilename(timestamp, sourceTable.Destination, strconv.Itoa(i), ".json.gz")
		outputFilenames = append(outputFilenames, outputName)
		log.InfoD("outputting-file", logger.M{"file-number": i, "location": outputName})

		// Gzip output into pipe so that we don't need to store locally
		reader, writer := io.Pipe()
		go func(index int) {
			zippedOutput, _ := gzip.NewWriterLevel(writer, gzip.BestSpeed) // sorcery
			if err != nil {
				log.ErrorD("compression-level-error", logger.M{"error": err.Error()})
				os.Exit(1)
			}

			sink := jsonsink.New(zippedOutput)
			// ALWAYS close the gzip first
			// (defer does LIFO)
			defer writer.Close()
			defer zippedOutput.Close()

			count, err := exportData(mongoSource, sourceTable, sink, timestamp)
			if err != nil {
				log.ErrorD("table-read-error", logger.M{"error": err.Error()})
				os.Exit(1)
			}
			log.InfoD("output-destination", logger.M{"collection": sourceTable.Destination, "count": count, "fileIndex": index})
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
	log.InfoD("output-total", logger.M{"rows": totalSummedRows, "files": numFiles})
	if totalSummedRows != totalMongoRows {
		log.ErrorD("rows-written-read-mismatch-error", logger.M{"written": totalMongoRows, "read": totalSummedRows})
		os.Exit(1)
	}
	// we always upload a manifest including the files we just created
	manifestFilename := formatFilename(timestamp, sourceTable.Destination, "", ".manifest")
	manifestReader, err := createManifest(flags.Bucket, outputFilenames)
	if err != nil {
		log.ErrorD("manifest-create-error", logger.M{"error": err.Error()})
		os.Exit(1)
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

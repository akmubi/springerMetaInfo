package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"unicode"
	"strings"
	"sync"
	"errors"
	"time"
	"flag"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const (
	springerAPIdomain	= "http://api.springernature.com/"
	apiKey				= "93934ee59a7e39e820351a39373c19e1"
)

// allowed charactes: '-', '_', '*', '"', '~', '.', '<', '>'
// Example: "**Springer Link**" -> "**Springer%20Link**"
func replaceNonLetters(source string) (result string) {

	for _, rune := range source {
		if ((rune == '-') || (rune == '*') || (rune == '_') ||
			(rune == '"') || (rune == '~') || (rune == '.')) ||
			(rune == '<') || (rune == '>') || (rune == ':') ||
			(unicode.IsLetter(rune)) || (unicode.IsDigit(rune)) {
			result += string(rune)
		} else {
			result += fmt.Sprintf("%%%X", int(rune))
		}
	}
	return
}

// "@sample string/hello !!!\u32a7" -> "sample_string_hello"
func MakeStringPretty(source string) (result string) {
	// source = removeForbiddenChars(source)
	for _, rune := range source {
		if (unicode.IsLetter(rune) || unicode.IsDigit(rune)) {
			result += string(rune)
		} else if unicode.IsSpace(rune) || rune == '\\' || rune == '/' {
			result += "_"
		} else {
			result += ""
		}
	}

	// " _sample_string_hello__" -> "sample_string_hello" 
	result = strings.Trim(result, " _")
	return
}

type SpringerResult struct {
	Total            int `xml:"total"`
	Start            int `xml:"start"`
	PageLength       int `xml:"pageLength"`
	RecordsDisplayed int `xml:"recordsDisplayed"`
}

type SpringerArticle struct {
	Title           string  	`xml:"title" json:"title"`
	Creators        []string	`xml:"creator" json:"authors"`
	PublicationName string  	`xml:"publicationName" json:"publication_name"`
	Volume          int     	`xml:"volume" json:"volume"`
	Number          string  	`xml:"number" json:"number"`
	OpenAccess		bool		`xml:"openAccess" json:"open_access"`
	StartingPage    int   		`xml:"startingPage" json:"starting_page"`
	EndingPage      int   		`xml:"endingPage" json:"ending_page"`
	Publisher       string		`xml:"publisher" json:"publisher"`
	PublicationDate string		`xml:"publicationDate" json:"publication_date"`
	URL             string		`xml:"url" json:"url"`
}

type SpringerRecord struct {
	Article  SpringerArticle `xml:"head>article" json:"article_info"`
	Abstract string          `xml:"body>p" json:"abstract"`
}

type SpringerResponse struct {
	XMLName xml.Name         `xml:"response"`
	Result  SpringerResult   `xml:"result"`
	Records []SpringerRecord `xml:"records>message" json:"articles"`
}

// database respresentation
type ArticleMetaInfo struct {
	Authors			[]string
	Keywords		[]string
	Title			string
	Abstract		string
	PublicationName string
	Number			string
	PublicationDate string
	Publisher		string
	Link			string
	PDFLink			string
	FileName		string
	OpenAccess		bool
	AlwaysTheSame	int
	StartingPage	int   
	EndingPage		int
	Volume			int   
	ID				int
}

func (a *ArticleMetaInfo) Convert(record SpringerRecord) {
	a.Authors = record.Article.Creators
	a.Title = record.Article.Title
	a.Abstract = record.Abstract
	a.PublicationName = record.Article.PublicationName
	a.Number = record.Article.Number
	a.PublicationDate = record.Article.PublicationDate
	a.Publisher = record.Article.Publisher
	a.Link = record.Article.URL
	keywords, err := parseKeywords(a.Link);
	if err != nil {
		log.Println("Parsing keywords:", err)
	}
	a.Keywords = keywords
	a.OpenAccess = record.Article.OpenAccess
	a.AlwaysTheSame = 1
	if a.OpenAccess {
		a.PDFLink = "https://link.springer.com/content/pdf/" + strings.Replace(strings.TrimPrefix(a.Link, "http://dx.doi.org/"), "/", "%2F", -1) + ".pdf"
	}
	a.Volume = record.Article.Volume
	a.StartingPage = record.Article.StartingPage
	a.EndingPage = record.Article.EndingPage
}

func formQuery(startPage int, searchQuery string) string {
	return springerAPIdomain +
		"metadata/pam" +
		"?q=" +
		searchQuery +
		"&s=" + strconv.Itoa(startPage) +
		"&p=" + strconv.Itoa(pageLength) +
		"&api_key=" + apiKey
}

var pageCounter, recordCounter int

func getPages(numworkers, numjobs int, jobs <- chan string, done chan<- error) <-chan SpringerRecord {
	records := make(chan SpringerRecord, numjobs * pageLength)

	for i := 0; i < numworkers; i++ {
		go func() {
			for j := range jobs {
				response, err := http.Get(j)
				if err != nil {
					done <- err
					continue
				}

				result, err := ioutil.ReadAll(response.Body)
				response.Body.Close()

				if err != nil {
					done <- err
					continue
				}

				var page SpringerResponse
				if err = xml.Unmarshal(result, &page); err != nil {
					done <- err
					continue
				}

				for _, record := range page.Records {
					records <- record
				}
				
				// update page counter
				var mutex sync.Mutex
				mutex.Lock()
				pageCounter++
				recordCounter += len(page.Records)
				mutex.Unlock()

				fmt.Printf("Passed: %d page(-s)\n", pageCounter)
				time.Sleep(timeoutDuration * time.Second)
				done <- nil
			}
		}()
	}
	return records
}

var itemCounter int

func storeMeta(database *DataBase, manager *S3Manager, numworkers, numjobs int, records <-chan SpringerRecord) <-chan error {
	done := make(chan error, numjobs * pageLength)
	fmt.Println("Starting uploading meta info and PDF files...")

	for i := 0; i < numworkers; i++ {
		go func(workerID int) {
			for record := range records {
				var articleMeta ArticleMetaInfo
				articleMeta.Convert(record)

				if needUpload && articleMeta.OpenAccess {

					filename := MakeStringPretty(articleMeta.Title) + ".pdf"

					response, err := http.Get(articleMeta.PDFLink)
					if err != nil {
						done <- err
						continue
					}

					pdfFile, err := os.Create(filename)
					if err != nil {
						done <- err
						continue
					}

					_, err = io.Copy(pdfFile, response.Body)
					response.Body.Close()
					
					if err != nil {
						done <- err
						pdfFile.Close()
						os.Remove(pdfFile.Name())
						continue
					}

					if _, err = pdfFile.Seek(0, io.SeekStart); err != nil {
						done <- err
						pdfFile.Close()
						os.Remove(pdfFile.Name())
						continue
					}

					// %PDF
					pdfSignature := make([]byte, 4)

					if _, err = pdfFile.Read(pdfSignature); err != nil {
						done <- err
						pdfFile.Close()
						os.Remove(pdfFile.Name())
						continue
					}

					if string(pdfSignature) != "%PDF" {
						done <- errors.New(fmt.Sprint("Not a PDF - ", articleMeta.PDFLink))
						pdfFile.Close()
						os.Remove(pdfFile.Name())
						continue
					}

					fmt.Println("Uploading -", filename)
					
					articleMeta.FileName = filename
					err = manager.UploadFile(bucketName, pdfFile)
					
					// close and remove
					pdfFile.Close()
					os.Remove(pdfFile.Name())

					if err != nil {
						done <- err
						continue
					}
				}

				// updating item counter
				var mutex sync.Mutex
				mutex.Lock()
				articleMeta.ID = itemCounter
				itemCounter++
				mutex.Unlock()

				err := database.PutItem(tableName, articleMeta)
				done <- err
			}
		}(i)
	}
	return done
}

func handleErrors(errorCount int, results <-chan error) (receivedErrors []error) {
	// receive results
	for i := 0; i < errorCount; i++ {
		workerErr := <-results
		if workerErr != nil {
			receivedErrors = append(receivedErrors, workerErr)
		}
	}
	return
}

var pageLength int
var timeoutDuration time.Duration

var tableName, primaryKey, primaryKeyType, sortKey, sortKeyType string 

var bucketName string
var needUpload bool

func main() {

	start := time.Now()

	// flags

	// searching
	pagesPtr			:= flag.Int		("records",		10,			"Number of records (meta info) in page (max - 50). Example: -records=35")
	constraintPtr		:= flag.Int		("maxpages",	100,		"Max number of pages to parse. If you want to parse all pages use -1. Example: -maxpages=200")
	keywordsPtr			:= flag.String	("keywords",	"",			"keywords to search in Springer. Example: -keywords=\"decompilation techniques\"")
	openAccessPtr		:= flag.Bool	("openaccess",	false,		"Parse only Open Access articles. Example: -openaccess")

	// database & s3
	tablenamePtr		:= flag.String	("tablename",	"", 		"Table name to upload into. Example: -tablename=\"Music\"")
	primaryKeyPtr		:= flag.String	("pkname",		"",			"Primary Key name. Example: -pkname=\"Publisher\"")
	primaryKeyTypePtr	:= flag.String	("pktype",		"",			"Primary Key type. Possible types - \"N\"/\"S\" (Number/String). Example: -pktype=N")
	sortKeyPtr			:= flag.String	("skname",		"",			"Sort Key name. Example: -skname=\"ID\"")
	sortKeyTypePtr		:= flag.String	("sktype",		"",			"Sort Key type. Possible types - \"N\"/\"S\" (Number/String). Example: -sktype=N")
	
	// credentials
	accessKeyPtr		:= flag.String	("accesskey",	"",			"Amazom DynamoDB Access Key ID")
	secretKeyPtr		:= flag.String	("secretkey",	"",			"Amazom DynamoDB Secret Access Key ID")
	regionPtr			:= flag.String	("region",		"",			"Amazom DynamoDB Region")
	
	// S3
	bucketNamePtr		:= flag.String	("bucketname", 	"",			"S3 bucket name to upload into. Example -bucketname=\"myuniquebucketname3287\"")

	// goroutines
	routinesPtr			:= flag.Int		("routines",	10,			"Number of routines. Example: -routines=30")
	timeoutPtr			:= flag.Int		("timeout",		1,			"Timeout duration in seconds (for each routine). Should be at least 1 second. Example: -timeout=5")

	flag.Parse()

	// keywords flag
	keywords := *keywordsPtr
	if keywords == "" {
		fmt.Fprintf(os.Stderr, "Keywords are not specified (Use -h or --help)")
		os.Exit(1)
	}

	// openaccess flag
	if *openAccessPtr {
		keywords += " openaccess:true"
	}

	// if needUpload {
	tableName, primaryKey, primaryKeyType = *tablenamePtr, *primaryKeyPtr, *primaryKeyTypePtr
	if !(tableName != "" && primaryKey != "" && (primaryKeyType == "N" || primaryKeyType == "S")) {
		if tableName == "" {
			fmt.Fprintf(os.Stderr, "Table name is not specified\n")
		}

		if primaryKey == "" {
			fmt.Fprintf(os.Stderr, "primary Key is not specified\n")
		}

		if primaryKeyType != "N" && primaryKeyType != "S" {
			fmt.Fprintf(os.Stderr, "Invalid primary key type - \"%s\"\n", primaryKeyType)
		}
		os.Exit(1)
	}

	fmt.Println("Table info:")
	fmt.Println("\tName:", tableName)
	fmt.Println("\tPrimary Key:", primaryKey)
	fmt.Println("\tPrimary Key Type:", primaryKeyType)

	sortKey, sortKeyType = *sortKeyPtr, *sortKeyTypePtr
	if sortKey != "" && sortKeyType != "" {
		fmt.Println("\tSort Key:", sortKey)
		fmt.Println("\tSort Key Type:", sortKeyType)
	}

	bucketName = *bucketNamePtr
	if bucketName != "" {
		needUpload = true
	}

	// page length flag 
	pageLength = *pagesPtr
	
	if pageLength > 50 {
		fmt.Fprintf(os.Stderr, "Page length is huge (%s)\n", pageLength)
		os.Exit(1)
	}
	
	// timeout flag 
	timeoutDuration = time.Duration(*timeoutPtr)
	
	if timeoutDuration < 1 {
		fmt.Fprintln(os.Stderr, "Invalid timeout value :", int(timeoutDuration))
		os.Exit(1)
	}

	// number of routines flag 
	numWorkers := *routinesPtr
	if numWorkers < 1 {
		fmt.Fprintln(os.Stderr, "Invalid routines number :", numWorkers)
		os.Exit(1)
	}

	// max pages flag 
	constraint := *constraintPtr
	if constraint < -1 {
		fmt.Fprintf(os.Stderr, "Warning: max number of pages (%d) is less than -1\n", constraint)
	}

	// connect to database before work
	var database DataBase
	var manager S3Manager

	fmt.Println("Connecting to database...")

	var accessKey, secretKey, region string = *accessKeyPtr, *secretKeyPtr, *regionPtr
	if accessKey == "" || secretKey == "" || region == "" {
		fmt.Println("Warning! Missing:")
		if accessKey == "" {
			fmt.Println("\tAccess Key ID")
		}

		if secretKey == "" {
			fmt.Println("\tSecret Key")
		}

		if region == "" {
			fmt.Println("\tRegion")
		}

		if accessKey == "" && secretKey == "" && region == "" {
			fmt.Println("Trying to find configuration in computer...")
			err := database.InitAuto()
			check(err)

			err = manager.InitAuto()
			check(err)
			fmt.Println("Found configuration")			
		} else {
			os.Exit(1)
		}
	} else {
		err := database.Init(accessKey, secretKey, region)
		check(err)

		if needUpload {
			err = manager.Init(accessKey, secretKey, region)
			check(err)
		}
	}

	fmt.Println("Checking table -", tableName)
	err := database.CreateTableIfNotExists(tableName, primaryKey, primaryKeyType, sortKey, sortKeyType)
	check(err)

	if needUpload {
		fmt.Println("Checking bucket -", bucketName)
		err = manager.CreateBucketIfNotExists(bucketName)
		check(err)
	}

	// we need to know how articles number
	// so get first page with total article count

	// join all strings in slice and process this string
	// example: ["cyber-physical" "system" "design"] --> "cyber-physical%20system%20design"
	searchQuery := replaceNonLetters(keywords)


	// form query
	query := formQuery(1, searchQuery)

	// send request
	springerResponse, err := http.Get(query)
	check(err)
	defer springerResponse.Body.Close()

	// read response
	xmlfile, err := ioutil.ReadAll(springerResponse.Body)
	check(err)
	
	if xmlfile == nil {
		log.Fatal("Empty response")
	}

	// unmarshal xml content
	var springerInfo SpringerResponse

	check(xml.Unmarshal(xmlfile, &springerInfo))

	// common structure with all metainfo

	fmt.Printf("Found %d records\n", springerInfo.Result.Total)

	// job count
	numJobs := constraint

	// if there is no constraints or total number of articles less than max number of articles 
	if numJobs < 0 || springerInfo.Result.Total / pageLength < constraint {
		numJobs = springerInfo.Result.Total / pageLength
		
		// additional records at last page
		if springerInfo.Result.Total % pageLength != 0 {
			numJobs++
		}
	}

	fmt.Println("Number of pages to parse:", numJobs)

	if numJobs > 0 {

		// urls 
		jobs			:= make(chan string, numJobs)

		// errors
		parserErrors	:= make(chan error, numJobs)

		// release workers
		records := getPages(numWorkers, numJobs, jobs, parserErrors)

		// send jobs
		go func() {
			for i := 0; i < numJobs; i++ {
				jobs <- formQuery((i * pageLength) + 1, searchQuery)
			}
			close(jobs)
		}()


		var awsErrors <-chan error

		awsErrors = storeMeta(&database, &manager, numWorkers, numJobs, records)

		// ---STOP HERE UNTIL ALL PARSER GOROUTINES FINISHED---
		// show parser errors
		var recievedParserErrors, receivedAWSErrors []error
		for i := 0; i < numJobs; i++ {
			if err := <- parserErrors; err != nil {
				recievedParserErrors = append(recievedParserErrors, err)
			}
		}

		if recievedParserErrors != nil {
			fmt.Println("\n", len(recievedParserErrors), " parser errors:")
			for _, err := range recievedParserErrors {
				fmt.Println(err)
			}
			fmt.Println()
		} else {
			fmt.Println("\nNo parser errors encountered\n")
		}

		// ---STOP HERE UNTIL ALL AWS GOROUTINES FINISHED---
		// show aws errors
		for i := 0; i < recordCounter; i++ {
			if err := <- awsErrors; err != nil {
				receivedAWSErrors = append(receivedAWSErrors, err)
			}
		}

		if receivedAWSErrors != nil {
			fmt.Println("\n", len(receivedAWSErrors), " AWS errors:")
			for _, err := range receivedAWSErrors {
				fmt.Println(err)
			}
			fmt.Println()
		} 
		fmt.Println("\nItems inserted into database -", itemCounter, "\n")

	} else if springerInfo.Result.Total > 0 {
		// else we have only 1 page that already parsed 
		for _, record := range springerInfo.Records {
			var articleMeta ArticleMetaInfo
			articleMeta.Convert(record)
			err = database.PutItem(tableName, articleMeta)
			check(err)
		}
	}
	fmt.Println("Success! Elapsed -", time.Since(start))
}
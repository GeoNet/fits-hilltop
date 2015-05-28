package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/AdRoll/goamz/aws"
	"github.com/GeoNet/goamz/sqs"
	"github.com/GeoNet/msg"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type HilltopValue struct {
	Timestamp time.Time
	Reading   float64
}

type HilltopMeasurement struct {
	SiteName   string `xml:"SiteName,attr"`
	DataSource struct {
		Name          string `xml:"Name,attr"`
		NumItems      string `xml:"NumItems,attr"`
		Interpolation string `xml:"Interpolation"`
	} `xml:"DataSource"`
	Data struct {
		DateFormat string   `xml:"DateFormat,attr"`
		Value      []string `xml:"V"`
	}
}

type Hilltop struct {
	XMLName     xml.Name             `xml:"Hilltop"`
	Agency      string               `xml:"Agency"`
	Measurement []HilltopMeasurement `xml:"Measurement"`
}

func (m *HilltopMeasurement) Values() ([]HilltopValue, error) {
	var values []HilltopValue
	for _, v := range m.Data.Value {
		parts := strings.Split(strings.TrimLeft(v, " "), " ")
		if len(parts) > 2 {
			t, err := time.Parse("02-Jan-06 15:04:05", fmt.Sprintf("%s %s", parts[0], parts[1]))
			if err != nil {
				return values, err
			}
			if m.Data.DateFormat == "UTC" {
				t = t.UTC()
			}
			r, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return values, err
			}
			values = append(values, HilltopValue{Timestamp: t, Reading: r})
		}
	}

	return values, nil
}

func (h *Hilltop) Observations(network, method string) ([]msg.Observation, error) {
	var msgs []msg.Observation
	for _, m := range h.Measurement {
		values, err := m.Values()
		if err != nil {
			return msgs, err
		}
		for _, v := range values {
			msgs = append(msgs, msg.Observation{
				NetworkID: network,
				SiteID:    m.SiteName,
				TypeID:    m.DataSource.Name,
				MethodID:  method,
				DateTime:  v.Timestamp,
				Value:     v.Reading,
				Error:     0.0,
			})
		}
	}

	return msgs, nil
}

func DecodeHilltopFile(file string) (*Hilltop, error) {

	xmlFile, err := os.Open(file)
	if err != nil {
		return nil, nil
	}
	defer xmlFile.Close()

	d := xml.NewDecoder(xmlFile)
	d.CharsetReader = CharsetReader

	h := Hilltop{}
	err = d.Decode(&h)
	if err != nil {
		return nil, err
	}

	return &h, nil
}

func main() {
	var Q *sqs.Queue

	// runtime settings
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "make noise")
	var dryrun bool
	flag.BoolVar(&dryrun, "dry-run", false, "don't actually send the messages")

	// amazon queue details
	var region string
	flag.StringVar(&region, "region", "", "provide AWS region, overides env variable \"AWS_REGION\"")
	var queue string
	flag.StringVar(&queue, "queue", "", "send messages to the SQS queue, overides env variable \"AWS_QUEUE\"")
	var key string
	flag.StringVar(&key, "key", "", "AWS access key id, overrides env and credentials file (default profile)")
	var secret string
	flag.StringVar(&secret, "secret", "", "AWS secret key id, overrides env and credentials file (default profile)")

	// required fits external values
	var method string
	flag.StringVar(&method, "method", "", "provide the FITS method")
	var network string
	flag.StringVar(&network, "network", "", "provide the FITS network")

	flag.Parse()

	// check required arguments
	if method == "" {
		log.Fatalf("no FITS method given")
	}
	if network == "" {
		log.Fatalf("no FITS network given")
	}

	// setup aws sqs queue
	if !dryrun {
		if region == "" {
			region = os.Getenv("AWS_FITS_REGION")
			if region == "" {
				log.Fatalf("unable to find region in environment or command line [AWS_FITS_REGION]")
			}
		}

		if queue == "" {
			queue = os.Getenv("AWS_FITS_QUEUE")
			if queue == "" {
				log.Fatalf("unable to find queue in environment or command line [AWS_FITS_QUEUE]")
			}
		}

		// configure amazon ...
		R := aws.GetRegion(region)

		// fall through to env then credentials file
		A, err := aws.GetAuth(key, secret, "", time.Now().Add(30*time.Minute))
		if err != nil {
			log.Fatalf("unable to get amazon auth: %s\n", err)
		}

		// create queue
		S := sqs.New(A, R)
		Q, err = S.GetQueue(queue)
		if err != nil {
			log.Fatalf("unable to get amazon queue: %s [%s/%s]\n", err, queue, region)
		}
	}

	// run through each provided file ...
	for _, f := range flag.Args() {
		if verbose {
			log.Printf("processing: %s\n", f)
		}

		// decode hilltop xml file
		h, err := DecodeHilltopFile(f)
		if err != nil {
			log.Fatalf("unable to decode hilltop xml file: %s [%s]\n", f, err)
		}

		// run through each observation
		obs, err := h.Observations(network, method)
		if err != nil {
			log.Fatalf("unable to recover hilltop observations: [%s]\n", err)
		}
		for _, m := range obs {
			mm, err := m.Encode()
			if err != nil {
				log.Fatalf("unable to encode hilltop msg: [%s]\n", err)
			}
			if verbose {
				log.Println(string(mm))
			}
			if !dryrun {
				_, err := Q.SendMessage(string(mm))
				if err != nil {
					log.Fatalf("unable to send hilltop msg: [%s]\n", err)
				}
			}
		}
		if verbose {
			log.Printf("completed\n")
		}
	}
}

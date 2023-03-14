package fetcher

import (
	"github.com/st8ed/aws-cost-exporter/pkg/state"

	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"database/sql"
	"encoding/csv"

	_ "github.com/mattn/go-sqlite3"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type ReportManifest struct {
	AssemblyId    string `json:"assemblyId"`
	Compression   string `json:"compression"`
	ContentType   string `json:"contentType"`
	BillingPeriod struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"billingPeriod"`
	Bucket     string   `json:"bucket"`
	ReportKeys []string `json:"reportKeys"`
}

type SortRecentFirst []state.BillingPeriod

func (a SortRecentFirst) Len() int           { return len(a) }
func (a SortRecentFirst) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortRecentFirst) Less(i, j int) bool { return a[i] < a[j] }
func index(slice []string, item string) int {
	for i := range slice {
			if slice[i] == item {
					return i
			}
	}
	return -1
}

func GetBillingPeriods(config *state.Config, client *s3.Client) ([]state.BillingPeriod, error) {
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(config.BucketName),
		Prefix:    aws.String("/" + config.ReportName + "/"),
		Delimiter: aws.String("/"),
	}

	periods := make([]state.BillingPeriod, 0)
	p := s3.NewListObjectsV2Paginator(client, params)

	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.CommonPrefixes {
			period, err := state.ParseBillingPeriod(
				strings.TrimSuffix(strings.TrimPrefix(*obj.Prefix, *params.Prefix), "/"),
			)

			if err != nil {
				return nil, err
			}

			periods = append(periods, *period)
		}
	}

	sort.Sort(SortRecentFirst(periods))

	if len(periods) > 3 {
		return periods[len(periods)-3:], nil
	} else {
		return periods, nil
	}
}

func GetReportManifest(config *state.Config, client *s3.Client, period *state.BillingPeriod, lastModified *time.Time) (*ReportManifest, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(config.BucketName),
		Key: aws.String(fmt.Sprintf(
			"/%s/%s/%s-Manifest.json",
			config.ReportName, string(*period), config.ReportName,
		)),
		IfModifiedSince: aws.Time(*lastModified),
	}

	obj, err := client.GetObject(context.TODO(), params)
	if err != nil {
		var ae smithy.APIError

		if !errors.As(err, &ae) {
			return nil, err
		}

		if ae.ErrorCode() == "NotModified" {
			return nil, nil
		} else {
			return nil, err
		}
	}
	defer obj.Body.Close()

	*lastModified = *obj.LastModified
	manifest := &ReportManifest{}

	decoder := json.NewDecoder(obj.Body)
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}

	if manifest.ContentType != "text/csv" {
		return nil, fmt.Errorf("report manifest contains unknown content type: %s", manifest.ContentType)
	}

	if manifest.Bucket != config.BucketName {
		return nil, fmt.Errorf("report manifest contains unexpected bucket name: %s", manifest.Bucket)
	}

	if len(manifest.ReportKeys) == 0 {
		return nil, fmt.Errorf("report manifest contains no report keys")
	}

	return manifest, nil
}

func FetchReport(config *state.Config, client *s3.Client, manifest *ReportManifest, logger log.Logger) error {
	periodStart, err := time.Parse("20060102T150405Z", manifest.BillingPeriod.Start)
	if err != nil {
		return err
	}

	reportFile := filepath.Join(
		config.RepositoryPath, "data",
		periodStart.Format("20060102")+"-"+manifest.AssemblyId+".csv",
	)

	if _, err := os.Stat(reportFile); !errors.Is(err, os.ErrNotExist) {
		level.Warn(logger).Log("msg", "Report file already exists, skipping download", "file", reportFile)
		return nil
	}

	level.Info(logger).Log("msg", "Fetching report", "file", reportFile, "parts", len(manifest.ReportKeys))

	for reportPart, reportKey := range manifest.ReportKeys {
		level.Info(logger).Log("msg", "Fetching report part", "file", reportFile, "part", reportPart)

		params := &s3.GetObjectInput{
			Bucket: aws.String(manifest.Bucket),
			Key:    aws.String(reportKey),
		}

		obj, err := client.GetObject(context.TODO(), params)
		if err != nil {
			return err
		}
		defer obj.Body.Close()

		level.Debug(logger).Log("ContentLength", obj.ContentLength)

		zr, err := gzip.NewReader(obj.Body)
		if err != nil {
			return err
		}
		defer zr.Close()

		r := csv.NewReader(zr)
		// Read the header row.
		header, err := r.Read()
		if err != nil {
			return err
		}
		bill_BillingPeriodStartDate := index(header, "bill/BillingPeriodStartDate")
		bill_BillingPeriodEndDate := index(header, "bill/BillingPeriodEndDate")
		product_ProductName := index(header, "product/ProductName")
		lineItem_Operation := index(header, "lineItem/Operation")
		lineItem_LineItemType := index(header, "lineItem/LineItemType")
		lineItem_UsageType := index(header, "lineItem/UsageType")
		pricing_unit := index(header, "pricing/unit")
		lineItem_CurrencyCode := index(header, "lineItem/CurrencyCode")

		db, err := sql.Open("sqlite3", config.DatabasePath)
		if err != nil {
			return err
		}
		defer db.Close()
		err = db.Ping()
		if err != nil {
			return err
		}
		for {
			record, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}

			level.Debug(logger).Log("SQLite", "Inserting record")

			stmt, err := db.Prepare(`insert into records (bill/BillingPeriodStartDate
				bill/BillingPeriodEndDate product/ProductName lineItem/Operation
				lineItem/LineItemType lineItem/UsageType pricing/unit lineItem/CurrencyCode) values(?, ?, ?, ?, ?, ?, ?, ?)`)
			if err != nil {
				return err
			}

			_, err = stmt.Exec(record[bill_BillingPeriodStartDate],
												 record[bill_BillingPeriodEndDate],
												 record[product_ProductName],
												 record[lineItem_Operation],
												 record[lineItem_LineItemType],
												 record[lineItem_UsageType],
												 record[pricing_unit],
												 record[lineItem_CurrencyCode])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func PrepareSqlite(config *state.Config, logger log.Logger) error {
	db, err := sql.Open("sqlite3", config.DatabasePath)
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return err
	}
	stmt, err := db.Prepare(`create table if not exists records (bill/BillingPeriodStartDate
														bill/BillingPeriodEndDate product/ProductName lineItem/Operation
														lineItem/LineItemType lineItem/UsageType pricing/unit lineItem/CurrencyCode)`)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

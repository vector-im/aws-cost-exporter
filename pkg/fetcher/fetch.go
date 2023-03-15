package fetcher

import (
	"github.com/st8ed/aws-cost-exporter/pkg/state"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	stsTypes "github.com/aws/aws-sdk-go-v2/service/sts/types"

	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"path/filepath"
	"sort"
	"strings"
	"math/rand"
	"database/sql"
	"encoding/csv"

	_ "github.com/mattn/go-sqlite3"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"io"
	"os"
	"time"

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

func RefreshClient(config *state.Config) (*s3.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Minute)
	defer cancel()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithDefaultRegion("us-east-1"))
	if err != nil {
		return nil, err
	}

	if (config.ChainedRole != "") {
		sourceAccount := sts.NewFromConfig(cfg)

		// Assume target role and store credentials
		rand.Seed(time.Now().UnixNano())
		response, err := sourceAccount.AssumeRole(ctx, &sts.AssumeRoleInput{
				RoleArn: aws.String(config.ChainedRole),
				RoleSessionName: aws.String("AWSCostExporter-" + strconv.Itoa(10000 + rand.Intn(25000))),
		})
		if err != nil {
			return nil, err
		}
		var assumedRoleCreds *stsTypes.Credentials = response.Credentials

		// Create config with target service client, using assumed role
		cfg, err = awsconfig.LoadDefaultConfig(ctx, awsconfig.WithDefaultRegion("us-east-1"),
																				awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*assumedRoleCreds.AccessKeyId, *assumedRoleCreds.SecretAccessKey, *assumedRoleCreds.SessionToken)))
		if err != nil {
			return nil, err
		}

	}
	return s3.NewFromConfig(cfg), nil
}

func GetBillingPeriods(config *state.Config) ([]state.BillingPeriod, error) {
	client, err := RefreshClient(config)
	if err != nil {
		return nil, err
	}
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

func GetReportManifest(config *state.Config, period *state.BillingPeriod, lastModified *time.Time) (*ReportManifest, error) {
	client, err := RefreshClient(config)
	if err != nil {
		return nil, err
	}
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

func FetchReport(config *state.Config, manifest *ReportManifest, logger log.Logger) error {
	client, err := RefreshClient(config)
	if err != nil {
		return err
	}
	periodStart, err := time.Parse("20060102T150405Z", manifest.BillingPeriod.Start)
	if err != nil {
		return err
	}

	for reportPart, reportKey := range manifest.ReportKeys {

		reportFile := filepath.Join(
			config.RepositoryPath, "data",
			periodStart.Format("20060102")+"-"+manifest.AssemblyId+"-"+strconv.FormatInt(int64(reportPart), 10)+".csv",
		)
		level.Info(logger).Log("msg", "Fetching report part", "file", reportFile, "part", reportPart)
		if _, err := os.Stat(reportFile); !errors.Is(err, os.ErrNotExist) {
			level.Warn(logger).Log("msg", "Report file part already exists, skipping download", "file", reportFile)
			continue
		}

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
		lineItem_UsageAmount := index(header, "lineItem/UsageAmount")
		pricing_unit := index(header, "pricing/unit")
		lineItem_CurrencyCode := index(header, "lineItem/CurrencyCode")
		lineItem_UnblendedCost := index(header, "lineItem/UnblendedCost")
		lineItem_UsageAccountId := index(header, "lineItem/UsageAccountId")

		db, err := sql.Open("sqlite3", config.DatabasePath)
		if err != nil {
			return err
		}
		defer func() {
			if err := db.Close(); err != nil {
				level.Warn(logger).Log("msg", "Unable to close database", "err", err)
			}
			level.Debug(logger).Log("msg", "Closed database")
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 15 * time.Minute)
		defer cancel()

    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()
		for {
			record, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}

			level.Debug(logger).Log("SQLite", "Inserting record", record[lineItem_UsageAccountId])

			stmt, err := tx.Prepare(`insert into records (bill_BillingPeriodStartDate,
				bill_BillingPeriodEndDate, product_ProductName, lineItem_Operation, lineItem_UnblendedCost, lineItem_UsageAccountId,
				lineItem_LineItemType, lineItem_UsageType, lineItem_UsageAmount, pricing_unit, lineItem_CurrencyCode)
				values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
			if err != nil {
				return err
			}

			_, err = stmt.Exec(record[bill_BillingPeriodStartDate],
												 record[bill_BillingPeriodEndDate],
												 record[product_ProductName],
												 record[lineItem_Operation],
												 record[lineItem_UnblendedCost],
												 record[lineItem_UsageAccountId],
												 record[lineItem_LineItemType],
												 record[lineItem_UsageType],
												 record[lineItem_UsageAmount],
												 record[pricing_unit],
												 record[lineItem_CurrencyCode])
			if err != nil {
				return err
			}
		}
		level.Debug(logger).Log("Report", "File done", reportFile)
		file, err := os.Create(reportFile)
		if err != nil {
				return err
		}
		defer file.Close()

    if err = tx.Commit(); err != nil {
			return err
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
	stmt, err := db.Prepare(`create table if not exists records (id integer primary key autoincrement, bill_BillingPeriodStartDate text,
														bill_BillingPeriodEndDate text, product_ProductName text,
														lineItem_Operation text, lineItem_UnblendedCost text, lineItem_UsageAccountId text,
														lineItem_LineItemType text, lineItem_UsageType text, lineItem_UsageAmount text, pricing_unit text, lineItem_CurrencyCode text)`)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

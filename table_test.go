package versionary

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type createTableRequest struct {
	TableName string `json:"TableName"`
}

type updateContinuousBackupsRequest struct {
	TableName                        string `json:"TableName"`
	PointInTimeRecoverySpecification struct {
		PointInTimeRecoveryEnabled bool  `json:"PointInTimeRecoveryEnabled"`
		RecoveryPeriodInDays       int32 `json:"RecoveryPeriodInDays"`
	} `json:"PointInTimeRecoverySpecification"`
}

func TestCreateTableEnablesPointInTimeRecovery(t *testing.T) {
	var (
		mu            sync.Mutex
		updateRequest updateContinuousBackupsRequest
		targets       []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		mu.Lock()
		targets = append(targets, target)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch target {
		case "DynamoDB_20120810.CreateTable":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read CreateTable request: %v", err)
				return
			}
			var request createTableRequest
			if err := json.Unmarshal(body, &request); err != nil {
				t.Errorf("decode CreateTable request: %v", err)
				return
			}
			_, _ = w.Write([]byte(`{"TableDescription":{"TableName":"things_test","TableStatus":"CREATING"}}`))
		case "DynamoDB_20120810.DescribeTable":
			_, _ = w.Write([]byte(`{"Table":{"TableName":"things_test","TableStatus":"ACTIVE"}}`))
		case "DynamoDB_20120810.UpdateContinuousBackups":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read UpdateContinuousBackups request: %v", err)
				return
			}
			if err := json.Unmarshal(body, &updateRequest); err != nil {
				t.Errorf("decode UpdateContinuousBackups request: %v", err)
				return
			}
			_, _ = w.Write([]byte(`{"ContinuousBackupsDescription":{"ContinuousBackupsStatus":"ENABLED","PointInTimeRecoveryDescription":{"PointInTimeRecoveryStatus":"ENABLED","RecoveryPeriodInDays":14}}}`))
		default:
			t.Errorf("unexpected DynamoDB operation %q", target)
			http.Error(w, "unexpected operation", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client := dynamodb.New(dynamodb.Options{
		BaseEndpoint: aws.String(server.URL),
		Credentials:  aws.AnonymousCredentials{},
		Region:       "us-west-2",
	})
	table := Table[struct{}]{
		Client:               client,
		EntityType:           "Thing",
		TableName:            "things_test",
		PointInTimeRecovery:  true,
		RecoveryPeriodInDays: 14,
		EntityRow:            TableRow[struct{}]{},
		IndexRows:            map[string]TableRow[struct{}]{},
	}

	if err := table.CreateTable(context.Background()); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if updateRequest.TableName != "things_test" {
		t.Errorf("UpdateContinuousBackups table = %q, want %q", updateRequest.TableName, "things_test")
	}
	if !updateRequest.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled {
		t.Error("PointInTimeRecoveryEnabled = false, want true")
	}
	if updateRequest.PointInTimeRecoverySpecification.RecoveryPeriodInDays != 14 {
		t.Errorf(
			"RecoveryPeriodInDays = %d, want 14",
			updateRequest.PointInTimeRecoverySpecification.RecoveryPeriodInDays,
		)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(targets) != 3 {
		t.Fatalf("DynamoDB operations = %v, want CreateTable, DescribeTable, UpdateContinuousBackups", targets)
	}
}

func TestUpdatePointInTimeRecoveryRetriesWhileContinuousBackupsAreUnavailable(t *testing.T) {
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		if attempts == 1 {
			w.Header().Set("X-Amzn-Errortype", "ContinuousBackupsUnavailableException")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"__type":"ContinuousBackupsUnavailableException","message":"backups are being enabled"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ContinuousBackupsDescription":{"ContinuousBackupsStatus":"ENABLED","PointInTimeRecoveryDescription":{"PointInTimeRecoveryStatus":"ENABLED","RecoveryPeriodInDays":14}}}`))
	}))
	defer server.Close()

	client := dynamodb.New(dynamodb.Options{
		BaseEndpoint: aws.String(server.URL),
		Credentials:  aws.AnonymousCredentials{},
		Region:       "us-west-2",
	})
	table := Table[struct{}]{
		Client:               client,
		TableName:            "things_test",
		PointInTimeRecovery:  true,
		RecoveryPeriodInDays: 14,
	}

	if err := table.updatePointInTimeRecovery(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("updatePointInTimeRecovery() error = %v", err)
	}
	if attempts != 2 {
		t.Errorf("UpdateContinuousBackups attempts = %d, want 2", attempts)
	}
}

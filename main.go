package streamsurfer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/google/uuid"
)

//go:generate mockery --name KinesisQueue --output ./mocks
type KinesisQueue interface {
	Enqueue(data map[string]any) error
	Flush() ([]any, error)
	Send(data map[string]any) error
}

type kinesisQueue struct {
	q             *queue.Queue
	maxSizeBytes  int
	currentSize   int
	lock          *sync.RWMutex
	kinesisClient *kinesis.Client
	streamName    string
	streamArn     string
	originApp     string
}

// New creates a new KinesisQueue for sending messages in a batch to a Kinesis stream.
//
// Parameters:
//
//	streamName: the name of the Kinesis stream to send messages to.
//
// Returns:
//
//	*kinesisQueue: a pointer to the concrete kinesisQueue implementation.
//	error: an error, if any occurred during the creation.
func New(streamName string) (*kinesisQueue, error) {
	return NewWithOpts(streamName, "sa-east-1", 1024, "", "")
}

// NewWithOrigin creates a new KinesisQueue for sending messages in a batch to a Kinesis stream.
//
// Parameters:
//
//	streamName: the name of the Kinesis stream to send messages to.
//	origin: the app name that will be used to identify the origin of the messages.
//
// Returns:
//
//	*kinesisQueue: a pointer to the concrete kinesisQueue implementation.
//	error: an error, if any occurred during the creation.
func NewWithOrigin(streamName string, origin string) (*kinesisQueue, error) {
	return NewWithOpts(streamName, "sa-east-1", 1024, origin, "")
}

// NewWithStreamArn creates a new KinesisQueue for sending messages in a batch to a Kinesis stream
// using the stream ARN. This method is useful to send messages to a stream in a different account.
//
// Parameters:
//
//	streamArn: the arn of the Kinesis stream to send messages to.
//	origin: the app name that will be used to identify the origin of the messages.
//
// Returns:
//
//	*kinesisQueue: a pointer to the concrete kinesisQueue implementation.
//	error: an error, if any occurred during the creation.
func NewWithStreamArn(streamArn, origin string) (*kinesisQueue, error) {
	if streamArn == "" {
		return &kinesisQueue{}, fmt.Errorf("streamArn must be provided")
	}

	streamName, err := extractStreamNameFromARN(streamArn)
	if err != nil {
		return &kinesisQueue{}, err
	}

	return NewWithOpts(streamName, "sa-east-1", 1024, origin, streamArn)
}

func extractStreamNameFromARN(arn string) (string, error) {
	parts := strings.Split(arn, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid ARN format")
	}
	return parts[1], nil
}

// NewWithOpts creates a new KinesisQueue for sending messages in a batch to a Kinesis stream.
//
// Parameters:
//
//	streamName: the name of the Kinesis stream to send messages to.
//	region: the aws region. The default is sa-east-1.
//	maxSizeKB: the maximum size in kilobytes for the batch.
//	origin: the app name that will be used to identify the origin of the messages.
//	streamArn: the ARN of the Kinesis stream to send messages to.
//
// Returns:
//
//	*kinesisQueue: a pointer to the concrete kinesisQueue implementation.
//	error: an error, if any occurred during the creation.
func NewWithOpts(streamName string, region string, maxSizeKB int, origin string, streamArn string) (*kinesisQueue, error) {
	if streamName == "" {
		return &kinesisQueue{}, fmt.Errorf("streamName must be provided")
	}

	if region == "" {
		region = "sa-east-1"
	}

	if maxSizeKB == 0 {
		return &kinesisQueue{}, fmt.Errorf("maxSizeKB must be provided")
	}

	kinesisClient, err := connectToKinesis(region)
	if err != nil {
		return &kinesisQueue{}, err
	}

	q := &kinesisQueue{
		q:             queue.New(0),
		maxSizeBytes:  maxSizeKB,
		lock:          &sync.RWMutex{},
		kinesisClient: kinesisClient,
		streamName:    streamName,
		originApp:     origin,
		streamArn:     streamArn,
	}
	return q, nil
}

func connectToKinesis(awsRegion string) (*kinesis.Client, error) {
	if awsRegion == "" {
		awsRegion = "sa-east-1"
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRetryMaxAttempts(5),
		config.WithRegion(awsRegion))
	if err != nil {
		return &kinesis.Client{}, err
	}

	return kinesis.NewFromConfig(cfg), nil
}

func (q *kinesisQueue) enrichAndValidate(data map[string]any) error {
	if _, ok := data["event"].(string); !ok {
		return fmt.Errorf("event field is required")
	}

	data["server_timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	if q.originApp != "" {
		data["origin"] = q.originApp
	}

	return nil
}

// Enqueue adds a new data item to the KinesisQueue for batch processing.
//
// Parameters:
//
//	data: a map containing the data to be enqueued. It must include an "event" field as a string.
//
// Returns:
//
//	error: an error, if any occurred during the enqueue process.
func (q *kinesisQueue) Enqueue(data map[string]any) error {
	if err := q.enrichAndValidate(data); err != nil {
		return err
	}

	dataBytes, _ := json.Marshal(data)
	itemSize := len(dataBytes)

	q.lock.Lock()

	var toFlush []any
	if q.currentSize+itemSize >= q.maxSizeBytes {
		toFlush = q.drainItems()
	}

	err := q.q.Put(data)
	if err != nil {
		q.lock.Unlock()
		return err
	}

	q.currentSize += itemSize
	q.lock.Unlock()

	if len(toFlush) > 0 {
		if _, err = q.sendToKinesis(toFlush); err != nil {
			return err
		}
	}

	return nil
}

// Send sends data directly to Kinesis without using the internal queue.
// Use this instead of Enqueue when low latency is required and batching is not needed.
func (q *kinesisQueue) Send(data map[string]any) error {
	if err := q.enrichAndValidate(data); err != nil {
		return err
	}

	_, err := q.sendToKinesis([]any{data})
	return err
}

// Flush sends the accumulated items in the KinesisQueue to the Kinesis stream.
//
// This method locks the KinesisQueue, processes the items, and sends them to the Kinesis stream.
// The items are retrieved from the queue and marshaled into JSON before being sent to Kinesis.
// If there are items in the queue, they are sent to Kinesis using the sendToKinesis method.
//
// Returns:
//
//	[]any: a slice of items that were sent to Kinesis.
//	error: an error, if any occurred during the flushing process.
func (q *kinesisQueue) Flush() ([]any, error) {
	q.lock.Lock()
	items := q.drainItems()
	q.lock.Unlock()

	if len(items) > 0 {
		return q.sendToKinesis(items)
	}

	return nil, nil
}

func (q *kinesisQueue) sendToKinesis(data []any) ([]any, error) {
	itemBytes, err := json.Marshal(data)
	if err != nil {
		return data, err
	}

	putRecord := kinesis.PutRecordInput{
		Data:         itemBytes,
		StreamName:   &q.streamName,
		PartitionKey: aws.String(uuid.New().String()),
	}

	// When streamArn available, add it to the record input
	if q.streamArn != "" {
		putRecord.StreamARN = &q.streamArn
	}

	_, err = q.kinesisClient.PutRecord(context.TODO(), &putRecord)
	if err != nil {
		return data, fmt.Errorf("failed to put record to kinesis: %v", err)
	}

	return nil, nil
}

// drainItems removes all items from the internal queue and resets currentSize.
// Caller must hold q.lock.
func (q *kinesisQueue) drainItems() []any {
	var items []any

	size := q.q.Len()

	for range size {
		val, err := q.q.Get(1)
		if err != nil {
			break
		}

		items = append(items, val[0])
	}

	q.currentSize = 0

	return items
}

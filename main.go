package streamsurfer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/google/uuid"
)

type KinesisQueue struct {
	q             *queue.Queue
	maxSizeBytes  int
	currentSize   int
	lock          *sync.RWMutex
	kinesisClient *kinesis.Client
	streamName    string
}

// New creates a new KinesisQueue for sending messages  in a batch to a Kinesis stream.
//
// Parameters:
//
//	streamName: the name of the Kinesis stream to send messages to.
//
// Returns:
//
//	*KinesisQueue: a pointer to the newly created KinesisQueue.
//	error: an error, if any occurred during the creation.
func New(streamName string) (*KinesisQueue, error) {
	return NewWithOpts(streamName, 1024)
}

// NewWithOpts creates a new KinesisQueue for sending messages  in a batch to a Kinesis stream.
//
// Parameters:
//
//	streamName: the name of the Kinesis stream to send messages to.
//	maxSizeKB: the maximum size in kilobytes for the batch.
//
// Returns:
//
//	*KinesisQueue: a pointer to the newly created KinesisQueue.
//	error: an error, if any occurred during the creation.
func NewWithOpts(streamName string, maxSizeKB int) (*KinesisQueue, error) {
	if streamName == "" {
		return &KinesisQueue{}, fmt.Errorf("streamName must be provided")
	}

	if maxSizeKB == 0 {
		return &KinesisQueue{}, fmt.Errorf("maxSizeKB must be provided")
	}

	kinesisClient, err := connectToKinesis()
	if err != nil {
		return &KinesisQueue{}, err
	}

	q := &KinesisQueue{
		q:             queue.New(0),
		maxSizeBytes:  maxSizeKB,
		lock:          &sync.RWMutex{},
		kinesisClient: kinesisClient,
		streamName:    streamName,
	}
	return q, nil
}

func connectToKinesis() (*kinesis.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRetryMaxAttempts(5))
	if err != nil {
		return &kinesis.Client{}, err
	}

	return kinesis.NewFromConfig(cfg), nil
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
func (q *KinesisQueue) Enqueue(data map[string]interface{}) error {
	if _, ok := data["event"].(string); !ok {
		return fmt.Errorf("event field is required")
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	// Add server timestamp to every event
	currentTime := time.Now().UTC()
	formattedTime := currentTime.Format("2006-01-02T15:04:05.999Z")
	data["server_timestamp"] = formattedTime

	dataBytes, _ := json.Marshal(data)

	itemSize := len(dataBytes)
	if q.currentSize+itemSize >= q.maxSizeBytes {
		_, err := q.flush()
		if err != nil {
			return err
		}
	}

	q.q.Put(data)
	q.currentSize += itemSize
	return nil
}

func (q *KinesisQueue) flush() ([]any, error) {
	var items []interface{}
	for q.currentSize > 0 {
		if val, err := q.q.Get(1); err == nil {
			items = append(items, val[0])

			itemBytes, _ := json.Marshal(val[0])
			itemSize := len(itemBytes)
			q.currentSize = q.currentSize - itemSize
		}
	}

	if len(items) > 0 {
		data, err := q.SendToKinesis(items)
		if err != nil {
			return data, err
		}
	}
	return nil, nil
}

// Flush sends the accumulated items in the KinesisQueue to the Kinesis stream.
//
// This method locks the KinesisQueue, processes the items, and sends them to the Kinesis stream.
// The items are retrieved from the queue and marshaled into JSON before being sent to Kinesis.
// If there are items in the queue, they are sent to Kinesis using the SendToKinesis method.
//
// Returns:
//
//	[]interface{}: a slice of items that were sent to Kinesis.
//	error: an error, if any occurred during the flushing process.
func (q *KinesisQueue) Flush() ([]any, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	var items []interface{}
	for q.currentSize > 0 {
		if val, err := q.q.Get(1); err == nil {
			items = append(items, val[0])

			itemBytes, _ := json.Marshal(val[0])
			itemSize := len(itemBytes)
			q.currentSize = q.currentSize - itemSize
		}
	}

	if len(items) > 0 {
		data, err := q.SendToKinesis(items)
		if err != nil {
			return data, err
		}
	}
	return nil, nil
}

func (q *KinesisQueue) SendToKinesis(data []any) ([]any, error) {
	itemBytes, err := json.Marshal(data)
	if err != nil {
		return data, err
	}

	_, err = q.kinesisClient.PutRecord(context.TODO(), &kinesis.PutRecordInput{
		Data:         itemBytes,
		StreamName:   &q.streamName,
		PartitionKey: aws.String(uuid.New().String()),
	})
	if err != nil {
		return data, fmt.Errorf("failed to put record to kinesis: %v", err)
	}

	return nil, nil
}

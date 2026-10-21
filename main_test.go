package streamsurfer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

const testStream = "test-stream"

// testEvent is about 1 KB, and carries seq so a test can tell events apart
// after they reach Kinesis.
func testEvent(seq int) map[string]any {
	return map[string]any{
		"event": "test.EventSent.v1",
		"data": map[string]any{
			"seq":     seq,
			"payload": strings.Repeat("x", 950),
		},
	}
}

func queuedSeq(item any) int {
	return item.(map[string]any)["data"].(map[string]any)["seq"].(int)
}

// fakeKinesis answers PutRecord and keeps the seq of every event it received.
// When release is set, it signals arrived and holds each answer until release
// is closed.
type fakeKinesis struct {
	mu      sync.Mutex
	puts    int
	seqs    []int
	arrived chan struct{}
	release chan struct{}
}

func (f *fakeKinesis) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var record struct {
		Data []byte
	}
	if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var batch []struct {
		Data struct {
			Seq int `json:"seq"`
		} `json:"data"`
	}
	if err := json.Unmarshal(record.Data, &batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.puts++
	for _, event := range batch {
		f.seqs = append(f.seqs, event.Data.Seq)
	}
	arrived, release := f.arrived, f.release
	f.mu.Unlock()

	if release != nil {
		arrived <- struct{}{}
		<-release
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	_, _ = w.Write([]byte(`{"SequenceNumber":"1","ShardId":"shardId-000000000000"}`))
}

// newTestQueue points the queue at a fake Kinesis. New would build the real
// AWS client, and its 1024-byte batch would flush on almost every Enqueue.
func newTestQueue(tb testing.TB, maxSizeBytes int) (*kinesisQueue, *fakeKinesis) {
	tb.Helper()

	kinesisServer := &fakeKinesis{}
	server := httptest.NewServer(kinesisServer)
	tb.Cleanup(server.Close)

	q := &kinesisQueue{
		maxSizeBytes: maxSizeBytes,
		lock:         &sync.RWMutex{},
		kinesisClient: kinesis.New(kinesis.Options{
			BaseEndpoint: aws.String(server.URL),
			Credentials:  aws.AnonymousCredentials{},
			Region:       "sa-east-1",
		}),
		streamName: testStream,
	}

	return q, kinesisServer
}

// drainItems runs with the lock held, so it hands the batch over as it is and
// leaves the queue empty for the next one.
func TestDrainItemsHandsOverEveryItemInOrder(t *testing.T) {
	q, _ := newTestQueue(t, 1<<20)

	const total = 50
	for seq := range total {
		if err := q.Enqueue(testEvent(seq)); err != nil {
			t.Fatalf("Error enqueuing a valid event: %v", err)
		}
	}

	q.lock.Lock()
	items := q.drainItems()
	again := q.drainItems()
	q.lock.Unlock()

	if len(items) != total {
		t.Fatalf("drained %d items, want %d", len(items), total)
	}
	for i, item := range items {
		if seq := queuedSeq(item); seq != i {
			t.Errorf("item %d has seq %d, want the items in enqueue order", i, seq)
		}
	}
	if q.currentSize != 0 {
		t.Errorf("currentSize is %d after a drain, want 0", q.currentSize)
	}
	if len(again) != 0 {
		t.Errorf("a second drain returned %d items, want none", len(again))
	}
}

// The drained batch is sent after the lock is released, so the next Enqueue
// must not write into it.
func TestDrainItemsDoesNotAliasTheNextBatch(t *testing.T) {
	q, _ := newTestQueue(t, 1<<20)

	for seq := range 2 {
		if err := q.Enqueue(testEvent(seq)); err != nil {
			t.Fatalf("Error enqueuing a valid event: %v", err)
		}
	}

	q.lock.Lock()
	drained := q.drainItems()
	q.lock.Unlock()

	for seq := 2; seq < 4; seq++ {
		if err := q.Enqueue(testEvent(seq)); err != nil {
			t.Fatalf("Error enqueuing a valid event: %v", err)
		}
	}

	for i, item := range drained {
		if seq := queuedSeq(item); seq != i {
			t.Errorf("drained item %d now has seq %d: the next batch wrote into it", i, seq)
		}
	}
}

// Enqueue counts an event's size when it arrives. If the caller changes the
// event afterwards, that count no longer matches the event, and Flush must
// still return.
func TestFlushReturnsWhenAQueuedEventShrank(t *testing.T) {
	q, kinesisServer := newTestQueue(t, 1<<20)

	event := testEvent(0)
	if err := q.Enqueue(event); err != nil {
		t.Fatalf("Error enqueuing a valid event: %v", err)
	}
	event["data"].(map[string]any)["payload"] = ""

	flushed := make(chan error, 1)
	go func() {
		_, err := q.Flush()
		flushed <- err
	}()

	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("Error flushing the batch: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not return: the drain is waiting for an item that is not in the queue")
	}

	kinesisServer.mu.Lock()
	defer kinesisServer.mu.Unlock()

	if len(kinesisServer.seqs) != 1 {
		t.Errorf("Kinesis received %d events, want the one queued", len(kinesisServer.seqs))
	}
}

// Enqueue sends a full batch after releasing the lock, so a slow PutRecord
// holds up only the caller whose event filled the batch.
func TestEnqueueSendsTheFullBatchAfterReleasingTheLock(t *testing.T) {
	q, kinesisServer := newTestQueue(t, 1536)

	kinesisServer.mu.Lock()
	kinesisServer.arrived = make(chan struct{}, 1)
	kinesisServer.release = make(chan struct{})
	kinesisServer.mu.Unlock()
	defer close(kinesisServer.release)

	if err := q.Enqueue(testEvent(0)); err != nil {
		t.Fatalf("Error enqueuing a valid event: %v", err)
	}
	go func() {
		_ = q.Enqueue(testEvent(1))
	}()

	select {
	case <-kinesisServer.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the full batch never reached Kinesis")
	}

	// Small enough to fit in the batch, so this Enqueue does not flush.
	small := map[string]any{"event": "test.EventSent.v1", "data": map[string]any{"seq": 2}}
	enqueued := make(chan error, 1)
	go func() {
		enqueued <- q.Enqueue(small)
	}()

	select {
	case err := <-enqueued:
		if err != nil {
			t.Fatalf("Error enqueuing a valid event: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Enqueue waited for another caller's PutRecord: the batch is sent with the lock held")
	}
}

// Each Enqueue that fills the batch flushes it after releasing the lock, while
// other goroutines keep enqueuing into the next one. Every event must reach
// Kinesis exactly once.
func TestConcurrentEnqueueSendsEveryEventOnce(t *testing.T) {
	q, kinesisServer := newTestQueue(t, 8*1024)

	const total = 300
	var wg sync.WaitGroup
	for seq := range total {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.Enqueue(testEvent(seq)); err != nil {
				t.Errorf("Error enqueuing a valid event: %v", err)
			}
		}()
	}
	wg.Wait()

	if _, err := q.Flush(); err != nil {
		t.Fatalf("Error flushing the last batch: %v", err)
	}

	kinesisServer.mu.Lock()
	defer kinesisServer.mu.Unlock()

	if kinesisServer.puts < 2 {
		t.Fatalf("Kinesis got %d PutRecord calls, want Enqueue to flush mid-run as well as the final Flush", kinesisServer.puts)
	}

	received := make(map[int]int, total)
	for _, seq := range kinesisServer.seqs {
		received[seq]++
	}
	if len(kinesisServer.seqs) != total {
		t.Errorf("Kinesis received %d events, want %d", len(kinesisServer.seqs), total)
	}
	for seq := range total {
		if received[seq] != 1 {
			t.Errorf("event %d reached Kinesis %d times, want exactly once", seq, received[seq])
		}
	}
}

// BenchmarkEnqueueBatch fills one 512 KB batch and drains it, the way a flush
// does. The drain runs with the lock held, so its share of the time is what
// every other Enqueue waits for.
func BenchmarkEnqueueBatch(b *testing.B) {
	const batchSize = 512 * 1024
	q, _ := newTestQueue(b, batchSize)

	for range b.N {
		for seq := 0; q.currentSize < batchSize-2048; seq++ {
			if err := q.Enqueue(testEvent(seq)); err != nil {
				b.Fatalf("Error enqueuing a valid event: %v", err)
			}
		}

		q.lock.Lock()
		q.drainItems()
		q.lock.Unlock()
	}
}

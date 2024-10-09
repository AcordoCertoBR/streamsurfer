# streamsurfer 🌊

`streamsurfer` is a Go module that provides functionality to send messages in batches to a Kinesis stream.

## Installation

To use `streamsurfer`, you need to have Go installed. You can install the module using the following command:

```bash
go get github.com/AcordoCertoBR/streamsurfer@v1.0.5
```

## Usage

Here is an example of how to use `streamsurfer`:

```go
package main

import (
	"fmt"
	"github.com/AcordoCertoBR/streamsurfer"
)

func main() {
	// Create a new KinesisQueue
	queue, err := streamsurfer.New("your-stream-name")
	if err != nil {
		fmt.Println("Error creating KinesisQueue:", err)
		return
	}

	// Defer the execution of flush
	defer func() {
		_, err := queue.Flush()
		if err != nil {
			fmt.Println("Error flushing queue:", err)
		}
	}()

	// Enqueue data
	data := map[string]interface{}{
		"event": "your-event-name", //required
		"custom": "data",
	}
	err = queue.Enqueue(data)
	if err != nil {
		fmt.Println("Error enqueuing data:", err)
		return
	}
}
```

In the example above, we create a new `KinesisQueue` instance, enqueue data, and defer the execution of the `Flush` method to send the accumulated items to the Kinesis stream.


### Note

Make sure to use `defer` to execute the `Flush` method after enqueuing data to ensure that the accumulated items are sent to the Kinesis stream efficiently.

## Default Queue Size and Custom Queue Size

The `streamsurfer` module provides a default queue size of 1024 kilobytes for batching messages to be sent to a Kinesis stream. This default size ensures that a reasonable amount of data can be accumulated before being flushed to the stream.

### Default Queue Size (1024 KB)

When you create a new `KinesisQueue` using the `New` function without specifying a custom size, the default queue size of 1024 KB is used. This default size is suitable for many use cases and helps in efficiently batching messages for processing.

### Custom Queue Size

If you have specific requirements or need to adjust the queue size based on your application's needs, you can use the `NewWithOpts` function to create a `KinesisQueue` with a custom queue size. By providing a custom size in kilobytes, you can fine-tune the batching behavior to better suit your workload.

Here's an example of creating a `KinesisQueue` with a custom queue size:

```go
queue, err := streamsurfer.NewWithOpts("your-stream-name", 2048)
```

In the example above, a `KinesisQueue` is created with a custom queue size of 2048 KB. Adjusting the queue size allows you to control how much data can be accumulated before triggering the flushing process to send the messages to the Kinesis stream.


## Special Thanks ❤️
@JoaoLeal92

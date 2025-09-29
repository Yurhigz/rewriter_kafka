# Kafka Replay Pipeline

High-performance Go pipeline for reading, transforming, and rewriting Kafka messages based on a daily timestamp with parallelized partition-based processing.

## Features

- **Parallel processing**: 8 Kafka partitions processed concurrently
- **Worker pool**: 16 workers for message processing
- **Auto-shutdown**: Inactivity detection and graceful pipeline shutdown
- **Real-time metrics**: Throughput monitoring (msg/s and MB/s)
- **Optimized memory**: Batch writing with Snappy compression
- **Zero goroutine leaks**: Clean shutdown of all channels

## Prerequisites

- Go 1.21+
- Kafka 2.8+
- Accessible Kafka broker (default: `localhost:19092`)

## Installation

```bash
git clone https://github.com/Yurhigz/rewriter_kafka.git
cd replay-script
go mod download
```

## Configuration

Main parameters are defined in `processing/processing.go`:

```go
const (
    TopicR_test        = "nst_bgp_MX204_ULIS"  // Source topic
    TopicW_test        = "test_1"               // Destination topic
    BrokerAddressLocal = "localhost:19092"      // Kafka address
    BatchSize          = 20000                  // Batch size
    Partitions         = 8                      // Number of partitions
    ProcessingWorkers  = 16                     // Number of workers
)
```

## Usage

### Simple start

```bash
go run main.go
```

### Build and run

```bash
go build -o kafka-replay
./kafka-replay
```

## Metrics

The pipeline displays metrics every 10 seconds:

```
Rate: 15420 msg/s, 45.23 MB/s | Total: 1542000 messages, 4.52 GB
```

## Auto-shutdown

The pipeline stops automatically in two cases:

1. **Prolonged inactivity**: No messages processed for 20 seconds (MetricsReporter)
2. **Global monitoring**: No activity detected for 15 seconds (main watchdog)

This allows batch ingestion processing without manual intervention.


### Data flow

1. **Reading**: Each `PartitionProcessor` reads from a specific Kafka partition
2. **Distribution**: Messages are sent to `processingQueue` (shared channel)
3. **Processing**: `ProcessingWorker`s transform messages in parallel
4. **Routing**: Results are routed to the correct partition channel
5. **Writing**: Each partition writes its results in batches to Kafka


## Performance

### Tested configuration

- **Machine**: 8 cores, 16GB RAM
- **Throughput**: ~32k msg/s, ~75 MB/s

### Tuning

To increase performance:

1. Increase `ProcessingWorkers` (based on your cores)
2. Increase `BatchSize` (watch memory usage)
3. Adjust `MinBytes/MaxBytes` in ReaderConfig
4. Reduce `BatchTimeout` for better responsiveness

## Acknowledgments

- [segmentio/kafka-go](https://github.com/segmentio/kafka-go) - Go Kafka client
- Go community for concurrency patterns
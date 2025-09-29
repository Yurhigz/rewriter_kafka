package processing

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	TopicR_test        = "nst_bgp_MX204_ULIS"
	TopicW_test        = "test_1"
	TopicR             = "raw_telemetry"
	TopicW             = "replay"
	BrokerAddressLocal = "localhost:19092"
	BatchSize          = 20000
	Partitions         = 8
	ProcessingWorkers  = 16
)

type ProcessingJob struct {
	Message     kafka.Message
	PartitionID int
}

var (
	MessagesProcessed int64
	bytesProcessed    int64
)

func PartitionProcessor(ctx context.Context, partition int, processingQueue chan ProcessingJob, resultChan chan kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	// Reader & writer avec la partition qui varie
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{BrokerAddressLocal},
		Topic:     TopicR,
		Partition: partition,
		MinBytes:  1e6,
		MaxBytes:  10e6,
		MaxWait:   100 * time.Millisecond,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(BrokerAddressLocal),
		Topic:        TopicW,
		Balancer:     &kafka.Hash{},
		BatchSize:    BatchSize / Partitions,
		BatchTimeout: 50 * time.Millisecond,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()
	// writer loop
	go func() {
		messageBatch := make([]kafka.Message, 0, BatchSize/Partitions)
		flushTicker := time.NewTicker(100 * time.Millisecond)
		defer flushTicker.Stop()

		for {
			select {
			case msg, ok := <-resultChan:
				if !ok {
					if len(messageBatch) > 0 {
						FlushBatch(ctx, writer, messageBatch)
					}
					return
				}

				messageBatch = append(messageBatch, msg)

				if len(messageBatch) >= cap(messageBatch) {
					FlushBatch(ctx, writer, messageBatch)
					messageBatch = messageBatch[:0]
				}

			case <-flushTicker.C:
				if len(messageBatch) > 0 {
					FlushBatch(ctx, writer, messageBatch)
					messageBatch = messageBatch[:0]
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// reader loop
	for {
		select {
		case <-ctx.Done():
			close(resultChan)
			return
		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Printf("Partition %d: arrêt propre", partition)
					close(resultChan)
					return
				}
				if err == context.DeadlineExceeded {
					log.Printf("Partition %d: timeout atteint", partition)
					close(resultChan)
					return
				}
				log.Printf("Partition %d read error: %v", partition, err)
				continue
			}

			select {
			case processingQueue <- ProcessingJob{Message: m, PartitionID: partition}:
			case <-ctx.Done():
				close(resultChan)
				return
			}
		}
	}
}

func ProcessingWorker(ctx context.Context, jobQueue chan ProcessingJob, resultChannels []chan kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-jobQueue:
			if !ok {
				return
			}

			transformedMsg := transformMessageOptimized(job.Message)

			select {
			case resultChannels[job.PartitionID] <- transformedMsg:
				atomic.AddInt64(&MessagesProcessed, 1)
				atomic.AddInt64(&bytesProcessed, int64(len(job.Message.Value)))
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func FlushBatch(ctx context.Context, writer *kafka.Writer, messages []kafka.Message) {
	if len(messages) == 0 {
		return
	}

	err := writer.WriteMessages(ctx, messages...)
	if err != nil {
		log.Printf("Error writing batch of %d messages: %v", len(messages), err)
	}
}

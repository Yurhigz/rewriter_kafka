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

	// Reader pour cette partition
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{BrokerAddressLocal},
		Topic:     TopicR_test,
		Partition: partition,
		MinBytes:  1e6,  // 1MB
		MaxBytes:  10e6, // 10MB
		MaxWait:   100 * time.Millisecond,
	})
	defer reader.Close()

	// Writer pour cette partition
	writer := &kafka.Writer{
		Addr:         kafka.TCP(BrokerAddressLocal),
		Topic:        TopicW_test,
		Balancer:     &kafka.Hash{},
		BatchSize:    BatchSize / Partitions, // Batch size par partition
		BatchTimeout: 50 * time.Millisecond,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	// Goroutine pour l'écriture
	go func() {
		messageBatch := make([]kafka.Message, 0, BatchSize/Partitions)
		flushTicker := time.NewTicker(100 * time.Millisecond)
		defer flushTicker.Stop()

		for {
			select {
			case msg, ok := <-resultChan:
				if !ok {
					// Canal fermé, flush final
					if len(messageBatch) > 0 {
						FlushBatch(ctx, writer, messageBatch)
					}
					return
				}

				messageBatch = append(messageBatch, msg)

				// Flush si batch plein
				if len(messageBatch) >= cap(messageBatch) {
					FlushBatch(ctx, writer, messageBatch)
					messageBatch = messageBatch[:0]
				}

			case <-flushTicker.C:
				// Flush périodique
				if len(messageBatch) > 0 {
					FlushBatch(ctx, writer, messageBatch)
					messageBatch = messageBatch[:0]
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// Boucle de lecture
	for {
		select {
		case <-ctx.Done():
			close(resultChan)
			return
		default:
			// Lecture bloquante simple
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

			// Envoyer au pool de workers
			select {
			case processingQueue <- ProcessingJob{Message: m, PartitionID: partition}:
				// Job envoyé
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

			// Envoyer le résultat au bon canal de partition
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

package main

import (
	"context"
	"fmt"
	"log"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
	"github.com/segmentio/kafka-go"
)

const (
	topicR             = "raw_telemetry"
	topicW             = "replay"
	brokerAddressLocal = "localhost:19092"
	batchSize          = 20000
	partitions         = 8
	processingWorkers  = 16
)

type ProcessingJob struct {
	Message     kafka.Message
	PartitionID int
}

var (
	messagesProcessed int64
	bytesProcessed    int64
)

func main() {

	// Reporter de métriques
	go metricsReporter()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Canal pour les jobs de processing
	processingQueue := make(chan ProcessingJob, batchSize)

	// Canal pour les résultats par partition
	resultChannels := make([]chan kafka.Message, partitions)
	for i := range resultChannels {
		resultChannels[i] = make(chan kafka.Message, batchSize/partitions)
	}

	var wg sync.WaitGroup

	// Pool de workers pour le processing
	for i := 0; i < processingWorkers; i++ {
		wg.Add(1)
		go processingWorker(ctx, processingQueue, resultChannels, &wg)
	}

	// Goroutines par partition (lecture + écriture)
	for partition := 0; partition < partitions; partition++ {
		wg.Add(1)
		go partitionProcessor(ctx, partition, processingQueue, resultChannels[partition], &wg)
	}

	wg.Wait()
}

func metricsReporter() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastTime := time.Now()
	var lastMessages, lastBytes int64

	for range ticker.C {
		now := time.Now()
		duration := now.Sub(lastTime).Seconds()

		currentMessages := atomic.LoadInt64(&messagesProcessed)
		currentBytes := atomic.LoadInt64(&bytesProcessed)

		msgRate := float64(currentMessages-lastMessages) / duration
		byteRate := float64(currentBytes-lastBytes) / duration / 1024 / 1024 // MB/s

		log.Printf("Rate: %.0f msg/s, %.2f MB/s | Total: %d messages, %.2f GB",
			msgRate, byteRate, currentMessages, float64(currentBytes)/1024/1024/1024)

		lastTime = now
		lastMessages = currentMessages
		lastBytes = currentBytes
	}
}

func partitionProcessor(ctx context.Context, partition int, processingQueue chan ProcessingJob, resultChan chan kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	// Reader pour cette partition
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddressLocal},
		Topic:     topicR,
		Partition: partition,
		MinBytes:  1e6,  // 1MB
		MaxBytes:  10e6, // 10MB
		MaxWait:   100 * time.Millisecond,
	})
	defer reader.Close()

	// Writer pour cette partition
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddressLocal),
		Topic:        topicW,
		Balancer:     &kafka.Hash{},
		BatchSize:    batchSize / partitions, // Batch size par partition
		BatchTimeout: 50 * time.Millisecond,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	// Goroutine pour l'écriture
	go func() {
		messageBatch := make([]kafka.Message, 0, batchSize/partitions)
		flushTicker := time.NewTicker(100 * time.Millisecond)
		defer flushTicker.Stop()

		for {
			select {
			case msg, ok := <-resultChan:
				if !ok {
					// Canal fermé, flush final
					if len(messageBatch) > 0 {
						flushBatch(ctx, writer, messageBatch)
					}
					return
				}

				messageBatch = append(messageBatch, msg)

				// Flush si batch plein
				if len(messageBatch) >= cap(messageBatch) {
					flushBatch(ctx, writer, messageBatch)
					messageBatch = messageBatch[:0]
				}

			case <-flushTicker.C:
				// Flush périodique
				if len(messageBatch) > 0 {
					flushBatch(ctx, writer, messageBatch)
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
				if err == context.Canceled || err == context.DeadlineExceeded {
					log.Println("Closing the channel")
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

func processingWorker(ctx context.Context, jobQueue chan ProcessingJob, resultChannels []chan kafka.Message, wg *sync.WaitGroup) {
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
				atomic.AddInt64(&messagesProcessed, 1)
				atomic.AddInt64(&bytesProcessed, int64(len(job.Message.Value)))
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func transformMessageOptimized(m kafka.Message) kafka.Message {
	origJSON := m.Value
	newJSON := make([]byte, 0, len(origJSON)+1024) // Buffer avec marge

	// Calculer une seule fois le timestamp de base pour aujourd'hui
	now := time.Now()
	todayBase := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	todayBaseNano := todayBase.UnixNano()

	jsonparser.ArrayEach(origJSON, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if err != nil {
			return
		}

		// Lire le timestamp original
		ts, err := jsonparser.GetInt(value, "timestamp")
		if err != nil {
			return
		}

		// Calcul optimisé du nouveau timestamp
		origTime := time.Unix(0, ts)
		timeInDay := int64(origTime.Hour())*3600000000000 +
			int64(origTime.Minute())*60000000000 +
			int64(origTime.Second())*1000000000 +
			int64(origTime.Nanosecond())

		newTimestamp := todayBaseNano + timeInDay

		// Conversion rapide du timestamp
		tsBytes := []byte(fmt.Sprintf("%d", newTimestamp))

		// Remplacer le timestamp
		modifiedValue, err := jsonparser.Set(value, tsBytes, "timestamp")
		if err != nil {
			return
		}

		// Construire le JSON final
		if len(newJSON) == 0 {
			newJSON = append(newJSON, '[')
		} else {
			newJSON = append(newJSON, ',')
		}
		newJSON = append(newJSON, modifiedValue...)
	})

	if len(newJSON) > 0 {
		newJSON = append(newJSON, ']')
	}

	return kafka.Message{
		Key:   m.Key,
		Value: newJSON,
	}
}

func flushBatch(ctx context.Context, writer *kafka.Writer, messages []kafka.Message) {
	if len(messages) == 0 {
		return
	}

	err := writer.WriteMessages(ctx, messages...)
	if err != nil {
		log.Printf("Error writing batch of %d messages: %v", len(messages), err)
	}
}

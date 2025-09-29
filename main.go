package main

import (
	"context"
	"log"
	_ "net/http/pprof"
	processing "replay-script/proccesing"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	// Reporter de métriques
	go processing.MetricsReporter()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Canal pour les jobs de processing
	processingQueue := make(chan processing.ProcessingJob, processing.BatchSize)

	// Canal pour les résultats par partition
	resultChannels := make([]chan kafka.Message, processing.Partitions)
	for i := range resultChannels {
		resultChannels[i] = make(chan kafka.Message, processing.BatchSize/processing.Partitions)
	}

	var wg sync.WaitGroup

	// Pool de workers pour le processing
	for i := 0; i < processing.ProcessingWorkers; i++ {
		wg.Add(1)
		go processing.ProcessingWorker(ctx, processingQueue, resultChannels, &wg)
	}

	// Goroutines par partition (lecture + écriture)
	var partitionWg sync.WaitGroup
	partitionWg.Add(processing.Partitions)
	for partition := 0; partition < processing.Partitions; partition++ {
		go func(p int) {
			defer partitionWg.Done()
			processing.PartitionProcessor(ctx, p, processingQueue, resultChannels[p], &partitionWg)
		}(partition)
	}

	go func() {
		lastProcessed := atomic.LoadInt64(&processing.MessagesProcessed)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			currentProcessed := atomic.LoadInt64(&processing.MessagesProcessed)
			if currentProcessed == lastProcessed && currentProcessed > 0 {
				log.Println("Aucune activité détectée depuis 10s, arrêt du pipeline...")
				cancel() // Déclenche l'arrêt de toutes les goroutines
				return
			}
			lastProcessed = currentProcessed
		}
	}()

	go func() {
		partitionWg.Wait()
		close(processingQueue)
		for _, channel := range resultChannels {
			close(channel)

		}
	}()
	wg.Wait()

}

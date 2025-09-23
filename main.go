package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/segmentio/kafka-go"
)

const (
	topicR             = "raw_telemetry"
	topicW             = "replay"
	brokerAddressLocal = "localhost:19092"
	batchSize          = 10000
	partitions         = 8
)

func main() {
	ctx := context.Background()
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddressLocal),
		Topic:        topicW,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		BatchTimeout: 100 * time.Millisecond,
	}
	defer writer.Close()

	var wg sync.WaitGroup
	for partition := 0; partition < partitions; partition++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   []string{brokerAddressLocal},
				Topic:     topicR,
				Partition: p,
				MinBytes:  10e6,
				MaxBytes:  10e6,
				MaxWait:   100 * time.Millisecond,
			})
			defer reader.Close()

			for {
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					log.Fatal("could not read message " + err.Error())
					break
				}

				origJSON := m.Value
				newJSON := make([]byte, 0, len(origJSON))

				_, _ = jsonparser.ArrayEach(origJSON, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
					// lire le timestamp
					ts, _ := jsonparser.GetInt(value, "timestamp")
					origTime := time.Unix(0, ts)
					now := time.Now()
					replayTime := time.Date(
						now.Year(), now.Month(), now.Day(),
						origTime.Hour(), origTime.Minute(), origTime.Second(),
						origTime.Nanosecond(),
						origTime.Location(),
					)

					// remplacer le timestamp dans l'élément
					value, _ = jsonparser.Set(value, []byte(fmt.Sprintf("%d", replayTime.UnixNano())), "timestamp")

					// construire la liste finale
					if len(newJSON) == 0 {
						newJSON = append(newJSON, '[')
					} else {
						newJSON = append(newJSON, ',')
					}
					newJSON = append(newJSON, value...)
				})

				if len(newJSON) > 0 {
					newJSON = append(newJSON, ']')
				}

				// écrire dans le topic replay
				err = writer.WriteMessages(ctx, kafka.Message{
					Key:   m.Key,
					Value: newJSON,
				})
				if err != nil {
					log.Fatal("could not write message " + err.Error())
					break
				}
			}

		}(partition)
	}
	wg.Wait()
}

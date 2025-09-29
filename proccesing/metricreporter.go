package processing

import (
	"log"
	"sync/atomic"
	"time"
)

func MetricsReporter() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastTime := time.Now()
	var lastMessages, lastBytes int64
	consecutiveZeros := 0
	const maxConsecutiveZeros = 2

	for range ticker.C {
		now := time.Now()
		duration := now.Sub(lastTime).Seconds()

		currentMessages := atomic.LoadInt64(&MessagesProcessed)
		currentBytes := atomic.LoadInt64(&bytesProcessed)

		msgRate := float64(currentMessages-lastMessages) / duration
		byteRate := float64(currentBytes-lastBytes) / duration / 1024 / 1024 // MB/s

		log.Printf("Rate: %.0f msg/s, %.2f MB/s | Total: %d messages, %.2f GB",
			msgRate, byteRate, currentMessages, float64(currentBytes)/1024/1024/1024)

		lastTime = now
		lastMessages = currentMessages
		lastBytes = currentBytes

		if msgRate == 0 && currentMessages > 0 {
			consecutiveZeros++
			log.Printf("Inactivité détectée (%d/%d)", consecutiveZeros, maxConsecutiveZeros)

			if consecutiveZeros >= maxConsecutiveZeros {
				log.Printf("Inactivité détectée (%d/%d)\n", consecutiveZeros, maxConsecutiveZeros)
				log.Println("MetricsReporter: arrêt automatique (fin du traitement)")
				return
			}
		} else if msgRate > 0 {
			consecutiveZeros = 0
		}
	}
}

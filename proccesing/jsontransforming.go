package processing

import (
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/segmentio/kafka-go"
)

func transformMessageOptimized(m kafka.Message) kafka.Message {
	origJSON := m.Value
	newJSON := make([]byte, 0, len(origJSON)+1024)

	// Calculer une seule fois le timestamp de base pour aujourd'hui
	now := time.Now()
	todayBase := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	todayBaseNano := todayBase.UnixNano()

	jsonparser.ArrayEach(origJSON, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if err != nil {
			return
		}

		ts, err := jsonparser.GetInt(value, "timestamp")
		if err != nil {
			return
		}

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

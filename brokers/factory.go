package brokers

import (
	"github.com/sunnyme20/marketconnector/brokers/angelone"
)

func NewBroker(brokerName string) (Broker, error) {

	switch brokerName {
	case "angelone":
		return &angelone.Angelone{}, nil
	default:
		return nil, nil
	}
}

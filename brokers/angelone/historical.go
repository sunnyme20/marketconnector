package angelone

import (
	"fmt"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

func (a *Angelone) GetHistoricalData(exchange models.Exchange, symbolToken string, interval models.Timeframe, fromDate, toDate string) (*models.Response[models.HistoricalResponse], error) {
	fmt.Printf("Fetching historical data for %s\n", a.ClientCode)

	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken

	// Fetch candle data
	req := HistoricalRequest{
		Exchange:    MapExchange(exchange),
		SymbolToken: symbolToken,
		Interval:    MapTimeframe(interval),
		FromDate:    fromDate,
		ToDate:      toDate,
	}

	var candleResp *HistoricalCandleData
	err := client.Post(Api.Historical, req, &candleResp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch candle data: %w", err)
	}

	// Parse candle data from [][]any to []HistoricalCandle
	var candles []models.HistoricalCandle
	for _, record := range candleResp.Data {
		if len(record) < 6 {
			continue
		}
		timestamp, _ := record[0].(string)
		open, _ := toFloat64(record[1])
		high, _ := toFloat64(record[2])
		low, _ := toFloat64(record[3])
		closeVal, _ := toFloat64(record[4])
		volume, _ := toInt64(record[5])

		candles = append(candles, models.HistoricalCandle{
			Timestamp: timestamp,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closeVal,
			Volume:    volume,
		})
	}

	// Fetch OI data
	var oiResp *HistoricalOIData
	err = client.Post(Api.HistoricalOI, req, &oiResp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch OI data: %w", err)
	}

	var oiItems []models.HistoricalOIItem
	for _, item := range oiResp.Data {
		oiItems = append(oiItems, models.HistoricalOIItem{
			Timestamp: item.Time,
			OI:        item.OI,
		})
	}

	finalResp := models.Response[models.HistoricalResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data: models.HistoricalResponse{
			Candles: candles,
			OI:      oiItems,
		},
	}
	return &finalResp, nil
}

func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func toInt64(val any) (int64, bool) {
	switch v := val.(type) {
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

package angelone

import (
	"fmt"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

func (a *Angelone) GetMarketQuote(mode models.QuoteMode, exchangeTokens map[models.Exchange][]string) (*models.Response[[]models.MarketQuoteResponse], error) {
	fmt.Printf("Fetching market quote for %s\n", a.ClientCode)

	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken

	// Convert common exchange keys to AngelOne API strings
	angelTokens := make(map[string][]string, len(exchangeTokens))
	for ex, tokens := range exchangeTokens {
		angelTokens[string(ex)] = tokens
	}

	req := QuoteRequest{
		Mode:           string(mode),
		ExchangeTokens: angelTokens,
	}

	var resp *QuoteResponse
	err := client.Post(Api.Quote, req, &resp)
	if err != nil {
		return nil, err
	}

	var quotes []models.MarketQuoteResponse
	for _, item := range resp.Data.Fetched {
		// Map depth from AngelOne format to common format
		var depth *models.MarketDepth
		if len(item.Depth.Buy) > 0 || len(item.Depth.Sell) > 0 {
			depth = &models.MarketDepth{}
			for _, d := range item.Depth.Buy {
				depth.Buy = append(depth.Buy, models.DepthItem{
					Quantity: int64(d.Quantity),
					Price:    d.Price,
					Orders:   d.Orders,
				})
			}
			for _, d := range item.Depth.Sell {
				depth.Sell = append(depth.Sell, models.DepthItem{
					Quantity: int64(d.Quantity),
					Price:    d.Price,
					Orders:   d.Orders,
				})
			}
		}

		quotes = append(quotes, models.MarketQuoteResponse{
			Exchange:      item.Exchange,
			TradingSymbol: item.TradingSymbol,
			LTP:           item.LTP,
			Open:          item.Open,
			High:          item.High,
			Low:           item.Low,
			Close:         item.Close,
			NetChange:     item.NetChange,
			PercentChange: item.PercentChange,
			AvgPrice:      item.AvgPrice,
			TradeVolume:   int64(item.TradeVolume),
			OpenInterest:  int64(item.OpnInterest),
			UpperCircuit:  item.UpperCircuit,
			LowerCircuit:  item.LowerCircuit,
			Depth:         depth,
		})
	}

	finalResp := models.Response[[]models.MarketQuoteResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data:    quotes,
	}
	return &finalResp, nil
}

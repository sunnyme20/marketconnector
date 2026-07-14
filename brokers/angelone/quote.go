package angelone

import (
	"fmt"
	"sort"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

const maxTokensPerRequest = 50

// fetchQuoteBatch calls the AngelOne quote API for a single batch of tokens.
func (a *Angelone) fetchQuoteBatch(mode models.QuoteMode, exchangeTokens map[string][]string) ([]models.MarketQuoteResponse, error) {
	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken

	req := QuoteRequest{
		Mode:           string(mode),
		ExchangeTokens: exchangeTokens,
	}

	var resp *QuoteResponse
	err := client.Post(Api.Quote, req, &resp)
	if err != nil {
		fmt.Println("Error fetching quote", err)
		return nil, err
	}

	var quotes []models.MarketQuoteResponse
	for _, item := range resp.Data.Fetched {
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
		// fmt.Println(item)
		quotes = append(quotes, models.MarketQuoteResponse{
			Exchange:      item.Exchange,
			TradingSymbol: item.TradingSymbol,
			SymbolToken:   item.SymbolToken,
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
	return quotes, nil
}

func (a *Angelone) GetMarketQuote(mode models.QuoteMode, exchangeTokens map[models.Exchange][]string) (*models.Response[[]models.MarketQuoteResponse], error) {
	fmt.Printf("Fetching market quote for %s\n", a.ClientCode)

	// Flatten all (exchange, token) pairs with deterministic exchange order.
	type pair struct {
		ex    string
		token string
	}
	exchanges := make([]string, 0, len(exchangeTokens))
	for ex := range exchangeTokens {
		exchanges = append(exchanges, string(ex))
	}
	sort.Strings(exchanges)

	var all []pair
	for _, exStr := range exchanges {
		tokens := exchangeTokens[models.Exchange(exStr)]
		for _, t := range tokens {
			all = append(all, pair{ex: exStr, token: t})
		}
	}

	// Batch into chunks of maxTokensPerRequest.
	var allQuotes []models.MarketQuoteResponse
	for i := 0; i < len(all); i += maxTokensPerRequest {
		end := i + maxTokensPerRequest
		if end > len(all) {
			end = len(all)
		}
		batch := make(map[string][]string)
		for _, p := range all[i:end] {
			batch[p.ex] = append(batch[p.ex], p.token)
		}
		quotes, err := a.fetchQuoteBatch(mode, batch)
		if err != nil {
			return nil, fmt.Errorf("batch %d: %w", i/maxTokensPerRequest, err)
		}
		allQuotes = append(allQuotes, quotes...)
	}

	return &models.Response[[]models.MarketQuoteResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data:    allQuotes,
	}, nil
}

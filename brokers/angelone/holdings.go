package angelone

import (
	"fmt"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

func (a *Angelone) GetHoldings() (*models.Response[[]models.HoldingResponse], error) {
	fmt.Printf("Fetching holdings for %s\n", a.ClientCode)

	var resp *Holdings

	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken
	err := client.Get(Api.Holding, nil, &resp)
	if err != nil {
		return nil, err
	}

	var holdings []models.HoldingResponse
	for _, item := range resp.Data {
		holdings = append(holdings, models.HoldingResponse{
			TradingSymbol: item.TradingSymbol,
			Exchange:      item.Exchange,
			T1Quantity:    item.T1Quantity,
			Quantity:      item.Quantity,
			Product:       item.Product,
			AveragePrice:  item.AveragePrice,
			LTP:           item.LTP,
			Close:         item.Close,
			Pnl:           item.Pnl,
			PnlPct:        item.PnlPct,
			Investment:    item.AveragePrice * float64(item.Quantity),
			Current:       item.Close * float64(item.Quantity),
			Return:        (item.Close - item.AveragePrice) * float64(item.Quantity),
		})
	}

	finalResp := models.Response[[]models.HoldingResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data:    holdings,
	}
	return &finalResp, nil
}

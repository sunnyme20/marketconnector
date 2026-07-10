package angelone

import (
	"fmt"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

func (a *Angelone) GetPositions() (*models.Response[[]models.PositionResponse], error) {
	fmt.Printf("Fetching positions for %s\n", a.ClientCode)

	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken

	var resp *PositionData
	err := client.Get(Api.Position, nil, &resp)
	if err != nil {
		return nil, err
	}

	var positions []models.PositionResponse
	for _, item := range resp.Data {
		positions = append(positions, models.PositionResponse{
			Exchange:      item.Exchange,
			SymbolToken:   item.SymbolToken,
			ProductType:   item.ProductType,
			TradingSymbol: item.TradingSymbol,
			BuyQty:        parseInt32(item.BuyQty),
			SellQty:       parseInt32(item.SellQty),
			BuyAmount:     parseFloat64(item.BuyAmount),
			SellAmount:    parseFloat64(item.SellAmount),
			BuyAvgPrice:   parseFloat64(item.BuyAvgPrice),
			SellAvgPrice:  parseFloat64(item.SellAvgPrice),
			AvgNetPrice:   parseFloat64(item.AvgNetPrice),
			NetValue:      parseFloat64(item.NetValue),
			CFBuyQty:      parseInt32(item.CFBuyQty),
			CFSellQty:     parseInt32(item.CFSellQty),
			CFBuyAmount:   parseFloat64(item.CFBuyAmount),
			CFSellAmount:  parseFloat64(item.CFSellAmount),
			LotSize:       parseInt32(item.LotSize),
		})
	}

	finalResp := models.Response[[]models.PositionResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data:    positions,
	}
	return &finalResp, nil
}

func parseInt32(s string) int32 {
	var val int32
	fmt.Sscanf(s, "%d", &val)
	return val
}

func parseFloat64(s string) float64 {
	var val float64
	fmt.Sscanf(s, "%f", &val)
	return val
}

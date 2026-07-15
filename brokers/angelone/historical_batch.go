package angelone

import (
	"context"
	"fmt"
	"sync"
	"time"

	models "github.com/sunnyme20/marketconnector/brokers/model"
	"golang.org/x/time/rate"
)

const angeloneDateFormat = "2006-01-02 15:04"

// SymbolRequest describes one symbol's historical data request.
type SymbolRequest struct {
	Exchange    models.Exchange
	SymbolToken string
	Interval    models.Timeframe
	FromDate    string
	ToDate      string
}

// SymbolBatch represents a single API-call-sized chunk for one symbol.
type SymbolBatch struct {
	Exchange    models.Exchange
	SymbolToken string
	Interval    models.Timeframe
	FromDate    string
	ToDate      string
}

// BatchResult holds the outcome of fetching one SymbolBatch.
type BatchResult struct {
	SymbolToken string
	FromDate    string
	ToDate      string
	Data        *models.Response[models.HistoricalResponse]
	Err         error
}

// HistoricalBatchItem holds the aggregated result for one symbol,
// using the same response envelope as GetHistoricalData.
type HistoricalBatchItem struct {
	SymbolToken string                     `json:"symbol_token"`
	Success     bool                       `json:"success"`
	Message     string                     `json:"message"`
	Broker      string                     `json:"broker"`
	Data        *models.HistoricalResponse `json:"data,omitempty"`
	Error       string                     `json:"error,omitempty"`
}

// splitDateRangeIntoBatches splits a single (fromDate, toDate) range into
// multiple SymbolBatch entries, each respecting the interval's max-days limit.
func (a *Angelone) splitDateRangeIntoBatches(
	exchange models.Exchange,
	symbolToken string,
	interval models.Timeframe,
	fromDate, toDate string,
) ([]SymbolBatch, error) {
	maxDays, ok := IntervalMaxDays[interval]
	if !ok {
		return nil, fmt.Errorf("unknown interval %q — no max-days mapping", interval)
	}

	from, err := time.Parse(angeloneDateFormat, fromDate)
	if err != nil {
		return nil, fmt.Errorf("invalid fromDate %q: %w", fromDate, err)
	}
	to, err := time.Parse(angeloneDateFormat, toDate)
	if err != nil {
		return nil, fmt.Errorf("invalid toDate %q: %w", toDate, err)
	}

	if to.Before(from) {
		return nil, fmt.Errorf("toDate %q is before fromDate %q", toDate, fromDate)
	}

	// Total calendar days (rounded up)
	totalDays := int(mathCeilDiv(to.Sub(from).Hours(), 24))
	if totalDays <= maxDays {
		return []SymbolBatch{{
			Exchange:    exchange,
			SymbolToken: symbolToken,
			Interval:    interval,
			FromDate:    fromDate,
			ToDate:      toDate,
		}}, nil
	}

	var batches []SymbolBatch
	currentFrom := from
	for currentFrom.Before(to) {
		currentTo := currentFrom.AddDate(0, 0, maxDays)
		if currentTo.After(to) {
			currentTo = to
		}

		batches = append(batches, SymbolBatch{
			Exchange:    exchange,
			SymbolToken: symbolToken,
			Interval:    interval,
			FromDate:    currentFrom.Format(angeloneDateFormat),
			ToDate:      currentTo.Format(angeloneDateFormat),
		})

		// Next batch starts 1 day after the previous chunk's end
		currentFrom = currentTo.AddDate(0, 0, 1)
	}

	return batches, nil
}

func mathCeilDiv(n float64, d float64) int {
	return int(mathCeil(n / d))
}

func mathCeil(x float64) int {
	if x == float64(int(x)) {
		return int(x)
	}
	if x > 0 {
		return int(x) + 1
	}
	return int(x)
}

// FetchHistoricalDataBatch fetches historical data for multiple symbols
// concurrently using a worker pool.
//
// Features:
//   - Automatically splits date ranges into per-interval max-day batches
//   - Runs up to 3 concurrent workers
//   - Rate-limits to 3 requests/second (AngelOne's limit)
//   - Aggregates results per symbol, merging candles/OI in date order
//
// Returns the same response envelope as GetHistoricalData, with Data being
// a list of per-symbol results.
func (a *Angelone) FetchHistoricalDataBatch(requests []SymbolRequest) (*models.Response[[]HistoricalBatchItem], error) {
	if len(requests) == 0 {
		return &models.Response[[]HistoricalBatchItem]{
			Success: true,
			Message: "SUCCESS",
			Broker:  "angelone",
			Data:    []HistoricalBatchItem{},
		}, nil
	}

	// ── Step 1: flatten every SymbolRequest into individual day-bounded batches ──
	var allBatches []SymbolBatch
	for _, req := range requests {
		batches, err := a.splitDateRangeIntoBatches(req.Exchange, req.SymbolToken, req.Interval, req.FromDate, req.ToDate)
		if err != nil {
			return nil, fmt.Errorf("split batch for %s: %w", req.SymbolToken, err)
		}
		allBatches = append(allBatches, batches...)
	}

	totalBatches := len(allBatches)
	if totalBatches == 0 {
		return &models.Response[[]HistoricalBatchItem]{
			Success: true,
			Message: "SUCCESS",
			Broker:  "angelone",
			Data:    []HistoricalBatchItem{},
		}, nil
	}

	// ── Step 2: channel-based worker pool ──
	const numWorkers = 3

	jobCh := make(chan SymbolBatch, totalBatches)
	resultCh := make(chan BatchResult, totalBatches)

	// Strict rate limiter — 3 req/s, burst=1 so only one request fires at a time
	limiter := rate.NewLimiter(HistoricalRateLimit, HistoricalRateBurst)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range jobCh {
				// Block until rate limiter allows the next request
				if err := limiter.Wait(context.Background()); err != nil {
					fmt.Printf("  [worker %d] ⚠️ rate limiter error: %v\n", workerID, err)
				}

				batchStart := time.Now()
				fmt.Printf("  [worker %d] fetching %s [%s → %s]\n", workerID, batch.SymbolToken, batch.FromDate, batch.ToDate)
				data, err := a.GetHistoricalData(
					batch.Exchange,
					batch.SymbolToken,
					batch.Interval,
					batch.FromDate,
					batch.ToDate,
				)
				elapsed := time.Since(batchStart).Truncate(time.Millisecond)
				if err != nil {
					fmt.Printf("  [worker %d] ❌ %s [%s → %s] after %v: %v\n", workerID, batch.SymbolToken, batch.FromDate, batch.ToDate, elapsed, err)
				} else if data != nil && data.Success {
					fmt.Printf("  [worker %d] ✅ %s [%s → %s] in %v: %d candles\n", workerID, batch.SymbolToken, batch.FromDate, batch.ToDate, elapsed, len(data.Data.Candles))
				} else {
					fmt.Printf("  [worker %d] ⚠️ %s [%s → %s] in %v: success=%v msg=%s\n", workerID, batch.SymbolToken, batch.FromDate, batch.ToDate, elapsed, data != nil && data.Success, data.Message)
				}
				resultCh <- BatchResult{
					SymbolToken: batch.SymbolToken,
					FromDate:    batch.FromDate,
					ToDate:      batch.ToDate,
					Data:        data,
					Err:         err,
				}
			}
		}(i)
	}

	// Feed all jobs into the channel
	for _, batch := range allBatches {
		jobCh <- batch
	}
	close(jobCh)

	// Wait for all workers to finish, then close results
	wg.Wait()
	close(resultCh)

	// ── Step 3: aggregate results per symbol ──
	itemsMap := make(map[string]*HistoricalBatchItem, len(requests))
	for res := range resultCh {
		item, ok := itemsMap[res.SymbolToken]
		if !ok {
			item = &HistoricalBatchItem{
				SymbolToken: res.SymbolToken,
				Broker:      "angelone",
			}
			itemsMap[res.SymbolToken] = item
		}
		if res.Err != nil {
			item.Success = false
			item.Message = "FAILED"
			item.Error = res.Err.Error()
			continue
		}
		if res.Data != nil && res.Data.Success {
			item.Success = true
			item.Message = "SUCCESS"
			if item.Data == nil {
				item.Data = &models.HistoricalResponse{}
			}
			item.Data.Candles = append(item.Data.Candles, res.Data.Data.Candles...)
			item.Data.OI = append(item.Data.OI, res.Data.Data.OI...)
		}
	}

	items := make([]HistoricalBatchItem, 0, len(itemsMap))
	for _, item := range itemsMap {
		items = append(items, *item)
	}

	return &models.Response[[]HistoricalBatchItem]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data:    items,
	}, nil
}

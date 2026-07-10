# MarketConnector

A **broker-agnostic** Go library for connecting to Indian stock brokers (Angel One, Zerodha, Upstox, etc.). Provides a unified interface for authentication, market data, portfolio management, and WebSocket streaming — write your application logic once, switch brokers with a single config change.

```go
broker, _ := brokers.NewBroker("angelone")
resp, _ := broker.NewSession("clientcode", "apikey", "password", "totp")
holdings, _ := broker.GetHoldings()
```

---

## Features

- **Unified Broker Interface** — Common API for login, profile, holdings, positions, quotes, historical data, and WebSocket streaming.
- **Pluggable Architecture** — Add new brokers by implementing the `Broker` interface. No changes needed in your application code.
- **WebSocket Support** — Real-time market data with automatic reconnection and exponential backoff.
- **Generic Response Types** — Type-safe `Response[T]` wrapper for consistent error handling across all brokers.
- **Go 1.26+ Generics** — Modern Go patterns throughout.

---

## Supported Brokers

| Broker | Status | Features |
|--------|--------|----------|
| Angel One | ✅ Complete | Login, Profile, Holdings, Positions, Quotes, Historical (candles + OI), WebSocket v2 (SmartAPI), RMS |
| Zerodha | ❌ Planned | — |
| Upstox | ❌ Planned | — |
| 5Paisa | ❌ Planned | — |
| ICICI Direct | ❌ Planned | — |

---

## Installation

```bash
go get github.com/sunnyme20/marketconnector
```

---

## Quick Start

### 1. Initialize a Broker

```go
package main

import (
    "fmt"
    "log"

    "github.com/sunnyme20/marketconnector/brokers"
    "github.com/sunnyme20/marketconnector/brokers/model"
)

func main() {
    broker, err := brokers.NewBroker("angelone")
    if err != nil {
        log.Fatal(err)
    }

    // Login
    resp, err := broker.NewSession("clientcode", "apikey", "password", "totp")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Logged in:", resp.Data.AccessToken)
}
```

### 2. Fetch Data

```go
// Holdings
holdings, _ := broker.GetHoldings()
for _, h := range holdings.Data {
    fmt.Printf("%s: %.2f\n", h.TradingSymbol, h.Investment)
}

// Positions
positions, _ := broker.GetPositions()

// Market Quotes
quotes, _ := broker.GetMarketQuote(model.QuoteModeFull, map[model.Exchange][]string{
    model.ExchangeNSE: {"2885", "1394"},
})

// Historical Data
hist, _ := broker.GetHistoricalData(
    "NSE", "2885",
    string(model.Timeframe1Day),
    "2026-06-01 09:00", "2026-07-01 15:30",
)
```

### 3. WebSocket Streaming

```go
ws, _ := broker.GetWebSocket()

ws.OnConnect(func() {
    fmt.Println("Connected!")
    ws.Subscribe(model.ModeLTP, []model.WSTokenGroup{
        {ExchangeType: model.NseCM, Tokens: []string{"2885", "1394"}},
    })
})

ws.OnTick(func(tick model.MarketQuoteResponse) {
    fmt.Printf("%s: %.2f\n", tick.TradingSymbol, tick.LTP)
})

ws.OnError(func(err error) {
    log.Println("WS Error:", err)
})

go ws.Serve()
time.Sleep(30 * time.Second)
ws.Stop()
```

---

## Project Structure

```
marketconnector/
├── main.go                          # Demo / entry point
├── go.mod
├── go.sum
│
├── brokers/
│   ├── broker.go                    # Broker interface (core contract)
│   ├── factory.go                   # NewBroker() factory
│   │
│   ├── model/                       # Broker-agnostic shared types
│   │   ├── common.go                #   Timeframe, Exchange, QuoteMode, WebSocketTicker
│   │   ├── request.go               #   LoginRequest
│   │   └── response.go              #   Response[T], Holding, Position, Quote, etc.
│   │
│   ├── database/                    # Symbol mapping abstraction (optional)
│   │   ├── db.go                    #   DB interface + factory
│   │   ├── postgres.go              #   PostgreSQL stub
│   │   ├── sqlite.go                #   SQLite stub
│   │   └── migration/               #   Schema migrations
│   │
│   └── angelone/                    # Angel One implementation
│       ├── client.go                #   HTTP client wrapper
│       ├── endpoints.go             #   API endpoint constants
│       ├── model.go                 #   AngelOne-specific types + mapping functions
│       ├── session.go               #   Login, profile, RMS, logout
│       ├── holdings.go              #   GetHoldings()
│       ├── positions.go             #   GetPositions()
│       ├── quote.go                 #   GetMarketQuote()
│       ├── historical.go            #   GetHistoricalData()
│       ├── websocket.go             #   Ticker — SmartAPI WebSocket v2
│       ├── charges.go               #   Brokerage / margin stubs
│       └── options.go               #   Option chain / OI stubs
```

---

## Architecture & Patterns

### Broker Interface

Every broker must implement `brokers.Broker` (`brokers/broker.go:1`):

```go
type Broker interface {
    // Session
    NewSession(clientcode, apikey, password, totp string) (*models.Response[models.LoginResponse], error)
    SetAccessToken(token string)
    SetFeedToken(token string)
    SetClientCode(code string)
    SetApiKey(key string)
    GetAccessToken() (string, error)

    // Account
    GetUserProfile() (*models.Response[models.UserProfileResponse], error)
    GetRMSData() (*models.Response[models.FundsResponse], error)
    Logout()

    // Portfolio
    GetHoldings() (*models.Response[[]models.HoldingResponse], error)
    GetPositions() (*models.Response[[]models.PositionResponse], error)

    // Market Data
    GetMarketQuote(mode models.QuoteMode, exchangeTokens map[models.Exchange][]string) (*models.Response[[]models.MarketQuoteResponse], error)
    GetHistoricalData(exchange, symbolToken, interval, fromDate, toDate string) (*models.Response[models.HistoricalResponse], error)

    // WebSocket
    GetWebSocket() (models.WebSocketTicker, error)

    // Orders (planned)
    PlaceOrder(...)
    ModifyOrder(...)
    CancelOrder(...)

    // Brokerage
    GetBrokerageCharges()
    GetMargin()
}
```

### Response Wrapper

All API responses use the generic wrapper (`brokers/model/response.go:1`):

```go
type Response[T any] struct {
    Success bool   `json:"success"`
    Message string `json:"message"`
    Broker  string `json:"broker"`
    Data    T      `json:"data"`
}
```

### WebSocket Ticker Interface

Real-time data consumers implement `models.WebSocketTicker` (`brokers/model/common.go:1`):

```go
type WebSocketTicker interface {
    Serve()
    Stop()
    Subscribe(mode int, tokenList []WSTokenGroup) error
    OnConnect(fn func())
    OnTick(fn func(MarketQuoteResponse))
    OnError(fn func(error))
    OnReconnect(fn func(attempt int, delay time.Duration))
    // ...
}
```

### Per-Broker Package Structure

Each broker lives in its own package under `brokers/<brokername>/`:

| File | Responsibility |
|------|---------------|
| `client.go` | HTTP client wrapper (headers, auth, base URL) |
| `endpoints.go` | All API endpoint URLs as constants |
| `model.go` | Broker-specific request/response structs + mapping functions |
| `session.go` | Main broker struct (`Angelone`, `Zerodha`, etc.) + methods for session/profile/RMS |
| `holdings.go` | `GetHoldings()` implementation |
| `positions.go` | `GetPositions()` implementation |
| `quote.go` | `GetMarketQuote()` implementation |
| `historical.go` | `GetHistoricalData()` implementation |
| `websocket.go` | WebSocket `Ticker` implementation |
| `charges.go` | Brokerage/margin calculation stubs |
| `options.go` | Option chain/OI stubs |

---

## Coding Conventions

### Naming

| Convention | Example |
|------------|---------|
| Exported = PascalCase | `NewSession`, `GetMarketQuote` |
| Unexported = camelCase | `getLocalIP`, `parseTick` |
| Acronyms uppercase | `ApiKey`, `AccessToken`, `FeedToken`, `LTP`, `RMS` |
| Json tags snake_case | `trading_symbol`, `symbol_token` |

### Error Handling

- Always return `(result, error)` — never swallow errors.
- Use `fmt.Errorf("...: %w", err)` to wrap errors with context.
- Parse failures should be logged or returned, not silently zeroed.

```go
// Good
func (a *Angelone) GetHoldings() (*models.Response[[]models.HoldingResponse], error) {
    var raw angelone.Holdings
    if err := a.client.Get(url, nil, &raw); err != nil {
        return nil, fmt.Errorf("holdings: %w", err)
    }
    // ...
}

// Avoid — silent parse failures
parseFloat64(s string) float64 { n, _ := strconv.ParseFloat(s, 64); return n }
```

### Imports

- Use import aliasing: `models "github.com/sunnyme20/marketconnector/brokers/model"`
- Group standard library, external, and internal imports with blank lines.

### Struct Embedding

- Broker-specific response types embed a common response struct for status/message/error.
- Use Go generics (`Response[T]`) for the public-facing API.
- Keep broker-internal types in the broker package; map to `models.*` types at the boundary.

### Testing

- Every broker implementation file should have a corresponding `*_test.go` file.
- Use table-driven tests for mapping/conversion functions.
- Use `httptest.Server` for testing HTTP clients.
- Use interface mocks for testing business logic without live API calls.

---

## How to Add a New Broker

1. **Create the package**
   ```
   brokers/<brokername>/
   ```

2. **Implement the files** following the per-broker structure above.

3. **Define broker-specific types** in `model.go` — request/response structs with `json` tags.

4. **Implement mapping functions** in `model.go` to convert:
   - Your broker's timeframe strings → `models.Timeframe`
   - Your broker's exchange codes → `models.Exchange`
   - Your broker's response structs → `models.Response[T]`

5. **Implement the `Broker` interface** on your main struct in `session.go`.

6. **Register the broker** in `brokers/factory.go`:
   ```go
   func NewBroker(name string) (Broker, error) {
       switch name {
       case "angelone":
           return &angelone.Angelone{}, nil
       case "zerodha":
           return &zerodha.Zerodha{}, nil  // add this line
       default:
           return nil, fmt.Errorf("unknown broker: %s", name)
       }
   }
   ```

7. **Write tests** — unit tests for mapping functions, integration test structure (credentials outside repo).

8. **Create a PR** 🚀

---

## Contributing

We welcome contributions! Here's how to get started:

### Development Setup

1. Fork the repository.
2. Clone your fork:
   ```bash
   git clone https://github.com/<your-username>/marketconnector.git
   ```
3. Install Go 1.26+.
4. Run the tests:
   ```bash
   go test ./...
   ```

### What to Work On

| Area | How to Help |
|------|-------------|
| **New Brokers** | Implement Zerodha, Upstox, 5Paisa, etc. |
| **Tests** | Add unit tests for existing AngelOne code (especially `parseTick` in `websocket.go`) |
| **Stubs** | Implement `GetBrokerageCharges()`, `GetMargin()`, `GetOptionChain()`, `GetOptionInterest()` |
| **Database** | Wire the `database` package into brokers for symbol mapping |
| **Bug Fixes** | Check [issues](https://github.com/sunnyme20/marketconnector/issues) |
| **Order APIs** | Add `PlaceOrder`, `ModifyOrder`, `CancelOrder` to the interface |

### Pull Request Process

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/zerodha-support
   ```
2. Write tests for your changes.
3. Ensure all tests pass:
   ```bash
   go test -v -race ./...
   ```
4. Run `gofmt` / `go vet`:
   ```bash
   go fmt ./... && go vet ./...
   ```
5. Open a PR with a clear title and description.

### Guidelines

- **No hardcoded credentials** — ever. Use environment variables or config files in examples.
- **Match the existing patterns** — file structure, error handling, naming conventions.
- **One feature per PR** — keep changes focused.
- **Document public API** — exported types and functions should have godoc comments.
- **No breaking changes** to the `Broker` interface without discussion.

---

## License

MIT — see [LICENSE](LICENSE).

---

## Disclaimer

This library is **not officially affiliated** with any broker. Use at your own risk. Market data and trading involve financial risk. Verify all data with your broker before making trading decisions.

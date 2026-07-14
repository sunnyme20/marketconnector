package angelone

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	models "github.com/sunnyme20/marketconnector/brokers/model"
)

// --------------- Error response ----------------------

// ErrorResponse represents a WebSocket error from AngelOne.
type ErrorResponse struct {
	CorrelationID string `json:"correlationID"`
	ErrorCode     string `json:"errorCode"`
	ErrorMessage  string `json:"errorMessage"`
}

// --------------- WebSocket constants -----------------

const (
	// Heartbeat interval – send "ping" every 10 seconds to keep the
	// connection alive even when the market is closed (no ticks).
	heartbeatInterval = 10 * time.Second

	// Auto reconnect defaults
	defaultReconnectMaxAttempts = 9999 // effectively unlimited
	reconnectMinDelay           = 5 * time.Second
	defaultReconnectMaxDelay    = 60 * time.Second
	defaultConnectTimeout       = 7 * time.Second
	connectionCheckInterval     = 15 * time.Second
	// Only kill an idle connection after 5 minutes — this prevents
	// premature disconnection during market closure or low-activity
	// periods, as heartbeats keep the connection alive anyway.
	dataTimeoutInterval = 5 * time.Minute

	// WebSocket URL
	defaultWSScheme = "wss"
	defaultWSHost   = "smartapisocket.angelone.in"
	defaultWSPath   = "/smart-stream"
)

// --------------- Ticker struct -----------------------

// Ticker is an AngelOne SmartAPI WebSocket v2 ticker instance.
type Ticker struct {
	Conn *websocket.Conn

	apiKey      string
	clientCode  string
	accessToken string
	feedToken   string

	url                 url.URL
	callbacks           callbacks
	lastPingTime        atomicTime
	autoReconnect       bool
	reconnectMaxRetries int
	reconnectMaxDelay   time.Duration
	connectTimeout      time.Duration

	reconnectAttempt int

	subscribedTokens []models.WSTokenGroup
	subscribedMode   int

	cancel context.CancelFunc
}

// atomicTime is wrapper over time.Time to safely access
// an updating timestamp concurrently.
type atomicTime struct {
	v atomic.Value
}

// Get returns the current timestamp.
func (b *atomicTime) Get() time.Time {
	return b.v.Load().(time.Time)
}

// Set sets the current timestamp.
func (b *atomicTime) Set(value time.Time) {
	b.v.Store(value)
}

// callbacks represents callbacks available in ticker.
type callbacks struct {
	onTick        func(models.MarketQuoteResponse)
	onMessage     func(int, []byte)
	onNoReconnect func(int)
	onReconnect   func(int, time.Duration)
	onConnect     func()
	onClose       func(int, string)
	onError       func(error)
}

// --------------- Constructor ------------------------

// NewWebSocket creates a new AngelOne WebSocket ticker instance.
func NewWebSocket(apiKey, clientCode, accessToken, feedToken string) *Ticker {
	return &Ticker{
		apiKey:              apiKey,
		clientCode:          clientCode,
		accessToken:         accessToken,
		feedToken:           feedToken,
		url:                 url.URL{Scheme: defaultWSScheme, Host: defaultWSHost, Path: defaultWSPath},
		autoReconnect:       true,
		reconnectMaxDelay:   defaultReconnectMaxDelay,
		reconnectMaxRetries: defaultReconnectMaxAttempts,
		connectTimeout:      defaultConnectTimeout,
	}
}

// --------------- Setters ----------------------------

// SetRootURL sets a custom WebSocket URL.
func (t *Ticker) SetRootURL(u url.URL) {
	t.url = u
}

// SetAccessToken sets the JWT access token.
func (t *Ticker) SetAccessToken(aToken string) {
	t.accessToken = aToken
}

// SetFeedToken sets the feed token.
func (t *Ticker) SetFeedToken(fToken string) {
	t.feedToken = fToken
}

// SetClientCode sets the client code (trading account id).
func (t *Ticker) SetClientCode(code string) {
	t.clientCode = code
}

// SetConnectTimeout sets default timeout for initial connect handshake.
func (t *Ticker) SetConnectTimeout(val time.Duration) {
	t.connectTimeout = val
}

// SetAutoReconnect enable/disable auto reconnect.
func (t *Ticker) SetAutoReconnect(val bool) {
	t.autoReconnect = val
}

// SetReconnectMaxDelay sets maximum auto reconnect delay.
func (t *Ticker) SetReconnectMaxDelay(val time.Duration) error {
	if val < reconnectMinDelay {
		return fmt.Errorf("ReconnectMaxDelay can't be less than %v", reconnectMinDelay)
	}
	t.reconnectMaxDelay = val
	return nil
}

// SetReconnectMaxRetries sets maximum reconnect attempts.
func (t *Ticker) SetReconnectMaxRetries(val int) {
	t.reconnectMaxRetries = val
}

// --------------- Callback setters -------------------

// OnConnect callback.
func (t *Ticker) OnConnect(f func()) {
	t.callbacks.onConnect = f
}

// OnError callback.
func (t *Ticker) OnError(f func(err error)) {
	t.callbacks.onError = f
}

// OnClose callback.
func (t *Ticker) OnClose(f func(code int, reason string)) {
	t.callbacks.onClose = f
}

// OnMessage callback.
func (t *Ticker) OnMessage(f func(messageType int, message []byte)) {
	t.callbacks.onMessage = f
}

// OnReconnect callback.
func (t *Ticker) OnReconnect(f func(attempt int, delay time.Duration)) {
	t.callbacks.onReconnect = f
}

// OnNoReconnect callback.
func (t *Ticker) OnNoReconnect(f func(attempt int)) {
	t.callbacks.onNoReconnect = f
}

// OnTick callback.
func (t *Ticker) OnTick(f func(tick models.MarketQuoteResponse)) {
	t.callbacks.onTick = f
}

// --------------- Serve / connection loop ------------

// Serve starts the connection to the ticker server. It is blocking, so run it in a goroutine.
func (t *Ticker) Serve() {
	t.ServeWithContext(context.Background())
}

// ServeWithContext starts the connection with a cancellable context.
func (t *Ticker) ServeWithContext(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if t.reconnectAttempt > t.reconnectMaxRetries {
				t.triggerNoReconnect(t.reconnectAttempt)
				return
			}

			if t.reconnectAttempt > 0 {
				nextDelay := time.Duration(math.Pow(2, float64(t.reconnectAttempt))) * time.Second
				if nextDelay > t.reconnectMaxDelay || nextDelay <= 0 {
					nextDelay = t.reconnectMaxDelay
				}
				t.triggerReconnect(t.reconnectAttempt, nextDelay)
				time.Sleep(nextDelay)
				if t.Conn != nil {
					t.Conn.Close()
				}
			}

			// For browser-based clients, query params can be used instead of headers.
			q := t.url.Query()
			q.Set("clientCode", t.clientCode)
			q.Set("feedToken", t.feedToken)
			q.Set("apiKey", t.apiKey)
			t.url.RawQuery = q.Encode()

			d := websocket.DefaultDialer
			d.HandshakeTimeout = t.connectTimeout

			// Use headers for authentication (preferred for non-browser clients)
			header := http.Header{}
			header.Set("Authorization", "Bearer "+t.accessToken)
			header.Set("x-api-key", t.apiKey)
			header.Set("x-client-code", t.clientCode)
			header.Set("x-feed-token", t.feedToken)

			fmt.Printf("[WS-ANGEL] Dialing %s\n", t.url.String())
			fmt.Printf("[WS-ANGEL] clientCode=%s feedToken=%s (len=%d)\n",
				t.clientCode, t.feedToken[:min(len(t.feedToken), 8)]+"...", len(t.feedToken))

			conn, _, err := d.Dial(t.url.String(), header)
			if err != nil {
				t.triggerError(fmt.Errorf("dial error: %w", err))
				if t.autoReconnect {
					t.reconnectAttempt++
					continue
				}
				return
			}

			t.Conn = conn

			defer func() {
				if t.Conn != nil {
					t.Conn.Close()
				}
			}()

			t.triggerConnect()

			// Resubscribe to stored tokens after reconnect
			if t.reconnectAttempt > 0 {
				if err := t.Resubscribe(); err != nil {
					t.triggerError(fmt.Errorf("resubscribe error: %w", err))
				}
			}

			t.reconnectAttempt = 0
			t.lastPingTime.Set(time.Now())

			var wg sync.WaitGroup

			wg.Add(1)
			go t.readMessage(ctx, &wg)

			wg.Add(1)
			go t.heartbeatLoop(ctx, &wg)

			if t.autoReconnect {
				wg.Add(1)
				go t.checkConnection(ctx, &wg)
			}

			wg.Wait()
		}
	}
}

// --------------- Heartbeat --------------------------

func (t *Ticker) heartbeatLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if t.Conn != nil {
				if err := t.Conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
					t.triggerError(fmt.Errorf("heartbeat write error: %w", err))
					return
				}
			}
		}
	}
}

// --------------- Connection watcher -----------------

func (t *Ticker) checkConnection(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(connectionCheckInterval)
			if time.Since(t.lastPingTime.Get()) > dataTimeoutInterval {
				if t.Conn != nil {
					t.Conn.Close()
				}
				t.reconnectAttempt++
				return
			}
		}
	}
}

// ------------------------ Read -----------------------

func (t *Ticker) readMessage(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			mType, msg, err := t.Conn.ReadMessage()
			if err != nil {
				t.triggerError(fmt.Errorf("read error: %w", err))
				return
			}

			t.lastPingTime.Set(time.Now())
			t.triggerMessage(mType, msg)

			switch mType {
			case websocket.TextMessage:
				text := string(msg)
				if text == "pong" {
					// Heartbeat response — keep alive
					break
				}
				fmt.Printf("[WS-RAW] text[%d]: %s\n", len(msg), text[:min(len(text), 200)])
				t.handleTextMessage(msg)
			case websocket.BinaryMessage:
				fmt.Printf("[WS-RAW] binary[%d]\n", len(msg))
				tick, err := parseTick(msg)
				if err != nil {
					t.triggerError(fmt.Errorf("parse error: %w", err))
					continue
				}
				t.triggerTick(tick)
			}
		}
	}
}

// --------------- Text message handling --------------

func (t *Ticker) handleTextMessage(msg []byte) {
	text := string(msg)

	// Heartbeat response
	if text == "pong" {
		return
	}

	// Error response
	var errResp ErrorResponse
	if err := json.Unmarshal(msg, &errResp); err == nil && errResp.ErrorCode != "" {
		t.triggerError(fmt.Errorf("server error [%s]: %s", errResp.ErrorCode, errResp.ErrorMessage))
		return
	}
}

// --------------- Binary parsing (AngelOne v2) -------

func parseTick(b []byte) (models.MarketQuoteResponse, error) {
	if len(b) < 2 {
		return models.MarketQuoteResponse{}, fmt.Errorf("packet too short: %d bytes", len(b))
	}

	subMode := int(b[0])

	// Token: 25-byte null-terminated string starting at byte 2
	tokenBytes := b[2:27]
	token := nullTerminatedString(tokenBytes)

	offset := 27

	var seqNum int64

	// Sequence number (int64, 8 bytes)
	if len(b) >= offset+8 {
		seqNum = int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	var exchTime int64

	// Exchange timestamp (int64, 8 bytes, epoch ms)
	if len(b) >= offset+8 {
		exchTime = int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	var ltp float64

	// LTP (int64, 8 bytes, in paise)
	if len(b) >= offset+8 {
		ltpVal := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		ltp = fromPaise(ltpVal)
	}
	offset += 8

	// Convert exchange timestamp from UTC epoch ms to readable IST string
	istLoc := time.FixedZone("IST", 5*60*60+30*60)
	exchangeTimeStr := time.UnixMilli(exchTime).In(istLoc).Format("02-Jan-2006 15:04:05")

	tick := models.MarketQuoteResponse{
		SymbolToken:    token,
		LTP:            ltp,
		SequenceNumber: seqNum,
		ExchangeTime:   exchangeTimeStr,
	}

	// For LTP mode (mode=1), packet ends here at 51 bytes
	if subMode == 1 {
		return tick, nil
	}

	var lastTradedQty int64

	// Last traded quantity (int64)
	if len(b) >= offset+8 {
		lastTradedQty = int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	var avgPrice float64

	// Average traded price (int64, paise)
	if len(b) >= offset+8 {
		avgVal := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		avgPrice = fromPaise(avgVal)
	}
	offset += 8

	var volume int64

	// Volume (int64)
	if len(b) >= offset+8 {
		volume = int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	var totBuyQty, totSellQty float64

	// Total buy quantity (double, 8 bytes)
	if len(b) >= offset+8 {
		totBuyQty = math.Float64frombits(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	// Total sell quantity (double, 8 bytes)
	if len(b) >= offset+8 {
		totSellQty = math.Float64frombits(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	var open, high, low, closeVal float64

	// Open price (int64, paise)
	if len(b) >= offset+8 {
		openVal := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		open = fromPaise(openVal)
	}
	offset += 8

	// High price (int64, paise)
	if len(b) >= offset+8 {
		highVal := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		high = fromPaise(highVal)
	}
	offset += 8

	// Low price (int64, paise)
	if len(b) >= offset+8 {
		lowVal := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		low = fromPaise(lowVal)
	}
	offset += 8

	// Close price (int64, paise)
	if len(b) >= offset+8 {
		closeRaw := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		closeVal = fromPaise(closeRaw)
	}
	offset += 8

	tick.LastTradedQty = lastTradedQty
	tick.AvgPrice = avgPrice
	tick.TradeVolume = volume
	tick.TotalBuyQty = totBuyQty
	tick.TotalSellQty = totSellQty
	tick.Open = open
	tick.High = high
	tick.Low = low
	tick.Close = closeVal
	tick.NetChange = ltp - closeVal

	if closeVal != 0 {
		tick.PercentChange = (ltp - closeVal) / closeVal * 100
	}

	// For Quote mode (mode=2), packet ends here at 123 bytes
	if subMode == 2 {
		return tick, nil
	}

	// SnapQuote fields (mode=3)

	var lastTradeTime int64

	// Last traded timestamp (int64, epoch ms)
	if len(b) >= offset+8 {
		lastTradeTime = int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	var oi int64

	// Open Interest (int64)
	if len(b) >= offset+8 {
		oi = int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
	}
	offset += 8

	tick.LastTradeTime = lastTradeTime
	tick.OpenInterest = oi

	// OI change % (double, dummy/garbage) – skip 8 bytes
	offset += 8

	// Best Five Data: 200 bytes (10 packets of 20 bytes each)
	if len(b) >= offset+200 {
		depth := &models.MarketDepth{}
		for i := 0; i < 5; i++ {
			item := parseDepthToCommon(b[offset+i*20 : offset+i*20+20])
			depth.Buy = append(depth.Buy, item)
		}
		offset += 100

		for i := 0; i < 5; i++ {
			item := parseDepthToCommon(b[offset+i*20 : offset+i*20+20])
			depth.Sell = append(depth.Sell, item)
		}
		offset += 100
		tick.Depth = depth
	}

	// Upper circuit (int64, paise)
	if len(b) >= offset+8 {
		uc := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		tick.UpperCircuit = fromPaise(uc)
	}
	offset += 8

	// Lower circuit (int64, paise)
	if len(b) >= offset+8 {
		lc := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		tick.LowerCircuit = fromPaise(lc)
	}
	offset += 8

	// 52 week high (int64, paise)
	if len(b) >= offset+8 {
		w52h := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		tick.Week52High = fromPaise(w52h)
	}
	offset += 8

	// 52 week low (int64, paise)
	if len(b) >= offset+8 {
		w52l := int64(binary.LittleEndian.Uint64(b[offset : offset+8]))
		tick.Week52Low = fromPaise(w52l)
	}

	return tick, nil
}

// parseDepthToCommon parses a 20-byte depth packet into a common DepthItem.
func parseDepthToCommon(b []byte) models.DepthItem {
	if len(b) < 20 {
		return models.DepthItem{}
	}
	qty := int64(binary.LittleEndian.Uint64(b[2:10]))
	priceRaw := int64(binary.LittleEndian.Uint64(b[10:18]))
	orders := int32(binary.LittleEndian.Uint16(b[18:20]))
	return models.DepthItem{
		Quantity: qty,
		Price:    fromPaise(priceRaw),
		Orders:   orders,
	}
}

// nullTerminatedString extracts a string from a byte buffer up to the first null byte.
func nullTerminatedString(b []byte) string {
	for i, v := range b {
		if v == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

// fromPaise converts paise (int64) to rupees (float64).
func fromPaise(val int64) float64 {
	return float64(val) / 100.0
}

// --------------- Subscribe / Unsubscribe ------------

// Subscribe subscribes to tokens with the given mode.
func (t *Ticker) Subscribe(mode int, tokenList []models.WSTokenGroup) error {
	if t.Conn == nil {
		// Store subscription for reconnection — the connection will resubscribe
		// automatically once it connects (via Resubscribe).
		t.subscribedTokens = tokenList
		t.subscribedMode = mode
		return nil
	}

	req := models.WSSubscribeRequest{
		Action: 1,
		Params: models.WSRequestParams{
			Mode:      mode,
			TokenList: tokenList,
		},
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	// Store subscription for reconnection
	t.subscribedTokens = tokenList
	t.subscribedMode = mode

	return t.Conn.WriteMessage(websocket.TextMessage, data)
}

// Unsubscribe unsubscribes from tokens.
func (t *Ticker) Unsubscribe(tokenList []models.WSTokenGroup) error {
	if t.Conn == nil {
		return nil
	}
	req := models.WSSubscribeRequest{
		Action: 0,
		Params: models.WSRequestParams{
			Mode:      t.subscribedMode,
			TokenList: tokenList,
		},
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	return t.Conn.WriteMessage(websocket.TextMessage, data)
}

// Resubscribe resubscribes to the previously stored tokens after reconnect.
func (t *Ticker) Resubscribe() error {
	if len(t.subscribedTokens) == 0 {
		return nil
	}
	return t.Subscribe(t.subscribedMode, t.subscribedTokens)
}

// --------------- Close / Stop -----------------------

// Close tries to close the connection gracefully.
func (t *Ticker) Close() error {
	return t.Conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

// Stop cancels the context and stops all goroutines.
func (t *Ticker) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
}

// --------------- Trigger callbacks ------------------

func (t *Ticker) triggerError(err error) {
	if t.callbacks.onError != nil {
		t.callbacks.onError(err)
	}
}

func (t *Ticker) triggerClose(code int, reason string) {
	if t.callbacks.onClose != nil {
		t.callbacks.onClose(code, reason)
	}
}

func (t *Ticker) triggerConnect() {
	if t.callbacks.onConnect != nil {
		t.callbacks.onConnect()
	}
}

func (t *Ticker) triggerReconnect(attempt int, delay time.Duration) {
	if t.callbacks.onReconnect != nil {
		t.callbacks.onReconnect(attempt, delay)
	}
}

func (t *Ticker) triggerNoReconnect(attempt int) {
	if t.callbacks.onNoReconnect != nil {
		t.callbacks.onNoReconnect(attempt)
	}
}

func (t *Ticker) triggerMessage(messageType int, message []byte) {
	if t.callbacks.onMessage != nil {
		t.callbacks.onMessage(messageType, message)
	}
}

func (t *Ticker) triggerTick(tick models.MarketQuoteResponse) {
	if t.callbacks.onTick != nil {
		t.callbacks.onTick(tick)
	}
}

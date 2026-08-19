package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	centrifugeplus "github.com/Xyncra/centrifuge-plus"
	"github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	// 支持通过环境变量配置 Jaeger 端点
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "localhost:4317"
	}

	exporter, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(jaegerEndpoint),
		otlptracegrpc.WithInsecure(),
	))
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("centrifuge-plus-example"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func main() {
	log.Println("=== centrifuge-plus Example ===")

	overallPass := true
	defer func() {
		if !overallPass {
			log.Println("=== Example FAILED ===")
			os.Exit(1)
		}
		log.Println("=== Example PASSED ===")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tp, err := initTracer(ctx)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer func() {
		if shutdownErr := tp.Shutdown(context.Background()); shutdownErr != nil {
			log.Printf("Warning: tracer shutdown: %v", shutdownErr)
		}
	}()

	if err := cleanupRedis(ctx, redisAddr); err != nil {
		log.Printf("Warning: cleanup redis: %v", err)
	}

	node, err := centrifuge.New(centrifuge.Config{})
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
		Address: redisAddr,
	})
	if err != nil {
		log.Fatalf("create shard: %v", err)
	}

	historyStore := newMemoryHistoryStore()

	broker, err := centrifugeplus.NewDualBroker(node, centrifugeplus.DualBrokerConfig{
		Live: centrifuge.RedisBrokerConfig{
			Prefix: "eg-live",
			Shards: []*centrifuge.RedisShard{redisShard},
		},
		Topic: centrifugeplus.TopicBrokerConfig{
			Prefix:       "eg-topic",
			RedisAddr:    redisAddr,
			RedisDB:      1,
			HistoryStore: historyStore,
			Tracing: centrifugeplus.TracingConfig{
				Enabled:  true,
				Provider: tp,
			},
		},
	})
	if err != nil {
		log.Fatalf("create broker: %v", err)
	}

	node.SetBroker(broker)

	for _, ch := range []string{"group-1", "channel-1", "channel-2"} {
		broker.RegisterChannelType("topic:"+ch, centrifugeplus.Topic)
		broker.RegisterChannelType("live:"+ch, centrifugeplus.Live)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		userID := e.Token
		if userID == "" {
			userID = e.ClientID
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: userID},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			ch := e.Channel
			if strings.HasPrefix(ch, "topic:") {
				broker.RegisterChannelType(ch, centrifugeplus.Topic)
				cb(centrifuge.SubscribeReply{
					Options: centrifuge.SubscribeOptions{
						EnableRecovery: true,
					},
				}, nil)
				return
			} else if strings.HasPrefix(ch, "live:") {
				broker.RegisterChannelType(ch, centrifugeplus.Live)
			}
			cb(centrifuge.SubscribeReply{}, nil)
		})
		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			if strings.HasPrefix(e.Channel, "topic:") {
				// "Persist first, then push" model:
				// 1. Pre-allocate offset
				// 2. Save to DB (in memoryHistoryStore for this example)
				// 3. Publish with pre-allocated offset (best-effort)
				// cb() is called only after persistence succeeds.
				workerTracer := tp.Tracer("example-persist")
				ctx, span := workerTracer.Start(context.Background(), "example.persist_and_push",
					trace.WithAttributes(attribute.String("channel", e.Channel)),
				)
				defer span.End()

				// Step 1: BatchIncrby
				positions, err := broker.BatchIncrby(ctx, []centrifugeplus.ChannelIncrbyRequest{{Channel: e.Channel}})
				if err != nil {
					log.Printf("[persist] BatchIncrby error: %v", err)
					span.SetStatus(codes.Error, err.Error())
					span.RecordError(err)
					cb(centrifuge.PublishReply{}, err)
					return
				}
				sp := positions[e.Channel]
				span.SetAttributes(
					attribute.Int64("offset", int64(sp.Offset)),
					attribute.String("epoch", sp.Epoch),
				)

				// Step 2: Save to DB (simulated with memoryHistoryStore)
				// In a real IM server, this would be a DB transaction:
				//   db.Begin() → Create(message) → Create(user_updates) → Commit()
				// If the transaction fails, the pre-allocated offset is wasted (gap).
				pub := &centrifuge.Publication{Data: e.Data}
				historyStore.SaveWithOffset(e.Channel, pub, uint32(sp.Offset))
				log.Printf("[persist] Saved channel=%s offset=%d", e.Channel, sp.Offset)

				// Step 3: PublishWithOffset (best-effort push, failure doesn't affect data consistency)
				if err := broker.PublishWithOffset(ctx, e.Channel, e.Data, centrifuge.PublishOptions{
					HistorySize: 100,
					HistoryTTL:  24 * time.Hour,
				}, sp); err != nil {
					log.Printf("[persist] PublishWithOffset error: %v", err)
					span.SetStatus(codes.Error, err.Error())
					span.RecordError(err)
					// Push failed but data is persisted. Client will discover via DB pull.
					// Still return success to cb — the publish is "done" from centrifuge's perspective.
				} else {
					span.SetStatus(codes.Ok, "")
				}

				cb(centrifuge.PublishReply{
					Options: centrifuge.PublishOptions{
						HistorySize: 100,
						HistoryTTL:  24 * time.Hour,
					},
				}, nil)
				return
			}
			cb(centrifuge.PublishReply{}, nil)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("run node: %v", err)
	}

	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", wsHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	httpServer := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("HTTP server on %s", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	if err := waitForServer(httpAddr, 5*time.Second); err != nil {
		log.Fatalf("server not ready: %v", err)
	}
	log.Println("=== Server Ready ===")

	time.Sleep(500 * time.Millisecond)

	rc, err := newRedisChecker(redisAddr, "eg-topic")
	if err != nil {
		log.Fatalf("create redis checker: %v", err)
	}
	defer rc.close()

	devices := []string{"user1-device-a", "user1-device-b", "user2-device-c", "user3-device-d"}
	var clients []*deviceClient
	for _, deviceID := range devices {
		dc, err := newDeviceClient(deviceID, serverURL)
		if err != nil {
			log.Fatalf("create client %s: %v", deviceID, err)
		}
		if err := dc.connect(ctx); err != nil {
			log.Fatalf("connect %s: %v", deviceID, err)
		}
		clients = append(clients, dc)
		log.Printf("Client connected: %s", deviceID)
	}
	time.Sleep(200 * time.Millisecond)

	testChannels := []string{"live:group-1", "live:channel-1", "live:channel-2"}
	for _, dc := range clients {
		for _, ch := range testChannels {
			if err := dc.subscribe(ctx, ch); err != nil {
				log.Printf("  %s subscribe %s: %v", dc.id, ch, err)
			}
		}
	}
	time.Sleep(200 * time.Millisecond)

	log.Println("=== Test 1: Live messages (real-time, non-persisted) ===")
	liveMsg1 := fmt.Sprintf(`{"text": "live message on group-1","ts":"%s"}`, time.Now().Format(time.RFC3339Nano))
	liveMsg2 := fmt.Sprintf(`{"text": "live message on channel-1","ts":"%s"}`, time.Now().Format(time.RFC3339Nano))
	liveMsg3 := fmt.Sprintf(`{"text": "live message on channel-2","ts":"%s"}`, time.Now().Format(time.RFC3339Nano))

	for _, dc := range clients {
		dc.clearMessages("live:group-1")
		dc.clearMessages("live:channel-1")
		dc.clearMessages("live:channel-2")
	}

	_ = clients[0].publish(ctx, "live:group-1", liveMsg1)
	_ = clients[0].publish(ctx, "live:channel-1", liveMsg2)
	_ = clients[0].publish(ctx, "live:channel-2", liveMsg3)
	time.Sleep(500 * time.Millisecond)

	for _, dc := range clients {
		for _, ch := range testChannels {
			msgs := dc.receivedMessages(ch)
			if len(msgs) < 1 {
				log.Printf("  FAIL: %s did not receive message on %s", dc.id, ch)
				overallPass = false
			}
		}
	}
	if overallPass {
		log.Println("  PASS: All clients received all live messages")
	}

	log.Println("=== Test 2: Topic messages (persisted + real-time) ===")
	topicChannels := []string{"topic:group-1", "topic:channel-1", "topic:channel-2"}
	for _, dc := range clients {
		for _, ch := range topicChannels {
			if err := dc.subscribe(ctx, ch); err != nil {
				log.Printf("  %s subscribe %s: %v", dc.id, ch, err)
			}
		}
	}
	time.Sleep(200 * time.Millisecond)

	for _, dc := range clients {
		for _, ch := range topicChannels {
			dc.clearMessages(ch)
		}
	}

	// [redis-check] Before publish: verify clean state
	log.Println("  [redis-check] BEFORE topic publish:")
	for _, ch := range topicChannels {
		rc.checkMetaExists(ctx, ch)
	}

	for _, ch := range topicChannels {
		msg := fmt.Sprintf(`{"text": "topic message on %s","ts":"%s"}`, ch, time.Now().Format(time.RFC3339Nano))
		_ = clients[0].publish(ctx, ch, msg)
	}
	time.Sleep(2 * time.Second)

	// [redis-check] After publish: verify meta created
	log.Println("  [redis-check] AFTER topic publish:")
	for _, ch := range topicChannels {
		rc.checkMetaExists(ctx, ch)
	}

	allPass := true
	for _, dc := range clients {
		for _, ch := range topicChannels {
			msgs := dc.receivedMessages(ch)
			if len(msgs) < 1 {
				log.Printf("  FAIL: %s did not receive topic message on %s", dc.id, ch)
				allPass = false
			}
		}
	}
	if allPass {
		log.Println("  PASS: All clients received all topic messages")
	} else {
		overallPass = false
	}

	// Verify history via HistoryStore
	topicStoreOK := true
	for _, ch := range topicChannels {
		pubs, err := historyStore.Query(context.Background(), ch, 0, 0)
		if err != nil {
			log.Printf("  FAIL: historyStore.Query(%s): %v", ch, err)
			topicStoreOK = false
		} else if len(pubs) != 1 {
			log.Printf("  FAIL: historyStore.Query(%s) expected 1 publication, got %d", ch, len(pubs))
			topicStoreOK = false
		} else {
			log.Printf("  PASS: historyStore has publication for channel %s (offset=%d)", ch, pubs[0].Offset)
		}
	}
	if topicStoreOK {
		log.Println("  PASS: All topic messages persisted to history store")
	} else {
		overallPass = false
	}

	log.Println("=== Test 3: Offline recovery ===")
	offlineClient := clients[3]
	log.Printf("Disconnecting %s...", offlineClient.id)
	offlineClient.disconnect()
	time.Sleep(200 * time.Millisecond)

	for _, dc := range clients[:3] {
		for _, ch := range topicChannels {
			dc.clearMessages(ch)
		}
	}
	for _, ch := range topicChannels {
		offlineClient.clearMessages(ch)
	}

	for _, ch := range topicChannels {
		msg := fmt.Sprintf(`{"text": "offline message on %s","ts":"%s"}`, ch, time.Now().Format(time.RFC3339Nano))
		_ = clients[0].publish(ctx, ch, msg)
	}
	time.Sleep(3 * time.Second)

	// [redis-check] After offline messages
	log.Println("  [redis-check] AFTER offline message processing:")
	for _, ch := range topicChannels {
		rc.checkMetaExists(ctx, ch)
	}

	log.Printf("Reconnecting %s...", offlineClient.id)
	if err := offlineClient.connect(ctx); err != nil {
		log.Printf("  reconnect %s: %v", offlineClient.id, err)
	}
	// Re-subscribe using cached Subscription objects (which track last offset for recovery)
	if err := offlineClient.reSubscribeAll(ctx); err != nil {
		log.Printf("  re-subscribe %s: %v", offlineClient.id, err)
	}
	time.Sleep(3 * time.Second)

	for _, ch := range topicChannels {
		msgs := offlineClient.receivedMessages(ch)
		log.Printf("  %s received %d messages on %s after reconnect (expected >= 1)", offlineClient.id, len(msgs), ch)
	}
	afterReconnect := len(offlineClient.receivedMessages("topic:group-1"))
	if afterReconnect >= 1 {
		log.Println("  PASS: Offline client recovered messages after reconnection")
	} else {
		log.Println("  FAIL: Offline client did not recover any messages")
		overallPass = false
	}

	// Verify offline messages were persisted to history store (now each topic channel has 2 publications)
	offlineStoreOK := true
	for _, ch := range topicChannels {
		pubs, err := historyStore.Query(context.Background(), ch, 0, 0)
		if err != nil {
			log.Printf("  FAIL: historyStore.Query(%s): %v", ch, err)
			offlineStoreOK = false
		} else if len(pubs) < 2 {
			log.Printf("  FAIL: historyStore.Query(%s) expected >=2 publications, got %d", ch, len(pubs))
			offlineStoreOK = false
		} else {
			log.Printf("  PASS: channel %s has %d persisted publications (offset=%d..%d)", ch, len(pubs), pubs[0].Offset, pubs[len(pubs)-1].Offset)
		}
	}
	if offlineStoreOK {
		log.Println("  PASS: All offline messages persisted to history store")
	} else {
		overallPass = false
	}

	log.Println("=== Test 4: Streaming text on Live channels (typewriter effect) ===")
	streamCh := "live:group-1"
	for _, dc := range clients {
		dc.clearMessages(streamCh)
	}

	words := []string{"Hello", " ", "world", " ", "this", " ", "is", " ", "streaming", " ", "text"}
	for _, word := range words {
		msg := fmt.Sprintf(`{"text": "%s","stream":true}`, word)
		_ = clients[0].publish(ctx, streamCh, msg)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	allPass = true
	for _, dc := range clients {
		msgs := dc.receivedMessages(streamCh)
		if len(msgs) < len(words) {
			log.Printf("  FAIL: %s received only %d/%d streaming words", dc.id, len(msgs), len(words))
			allPass = false
		}
	}
	if allPass {
		log.Println("  PASS: All clients received full streaming text")
	} else {
		overallPass = false
	}

	log.Println("=== Example complete ===")

	for _, dc := range clients {
		dc.disconnect()
		dc.close()
	}
	_ = httpServer.Shutdown(context.Background())
	_ = node.Shutdown(context.Background())
	_ = broker.Close(context.Background())
}

type deviceClient struct {
	id       string
	client   *centrifugego.Client
	mu       sync.Mutex
	received map[string][]string
	subs     map[string]*centrifugego.Subscription
}

func newDeviceClient(id string, endpoint string) (*deviceClient, error) {
	client := centrifugego.NewJsonClient(endpoint, centrifugego.Config{
		Token: id,
	})
	return &deviceClient{
		id:       id,
		client:   client,
		received: make(map[string][]string),
		subs:     make(map[string]*centrifugego.Subscription),
	}, nil
}

func (c *deviceClient) connect(ctx context.Context) error {
	return c.client.Connect()
}

func (c *deviceClient) subscribe(ctx context.Context, channel string) error {
	c.mu.Lock()
	if _, ok := c.subs[channel]; ok {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	sub, err := c.client.NewSubscription(channel)
	if err != nil {
		return fmt.Errorf("new sub: %w", err)
	}
	sub.OnPublication(func(e centrifugego.PublicationEvent) {
		c.mu.Lock()
		c.received[channel] = append(c.received[channel], string(e.Data))
		c.mu.Unlock()
	})
	if err := sub.Subscribe(); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	c.mu.Lock()
	c.subs[channel] = sub
	c.mu.Unlock()
	return nil
}

func (c *deviceClient) publish(ctx context.Context, channel string, text string) error {
	_, err := c.client.Publish(ctx, channel, []byte(text))
	return err
}

func (c *deviceClient) disconnect() {
	_ = c.client.Disconnect()
}

func (c *deviceClient) receivedMessages(channel string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]string, len(c.received[channel]))
	copy(result, c.received[channel])
	return result
}

func (c *deviceClient) reSubscribeAll(ctx context.Context) error {
	c.mu.Lock()
	subs := make([]*centrifugego.Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.mu.Unlock()

	for _, sub := range subs {
		if err := sub.Subscribe(); err != nil {
			return fmt.Errorf("resubscribe: %w", err)
		}
	}
	return nil
}

func (c *deviceClient) clearMessages(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.received, channel)
}

func (c *deviceClient) close() {
	c.client.Close()
}

type memoryHistoryStore struct {
	mu      sync.RWMutex
	data    map[string][]*centrifuge.Publication
	offsets map[string]uint32
}

func newMemoryHistoryStore() *memoryHistoryStore {
	return &memoryHistoryStore{
		data:    make(map[string][]*centrifuge.Publication),
		offsets: make(map[string]uint32),
	}
}

func (s *memoryHistoryStore) Query(_ context.Context, channel string, sinceOffset uint32, _ uint32) ([]*centrifuge.Publication, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pubs := s.data[channel]
	if sinceOffset == 0 {
		return pubs, nil
	}
	var result []*centrifuge.Publication
	for _, pub := range pubs {
		if pub.Offset > uint64(sinceOffset) {
			result = append(result, pub)
		}
	}
	return result, nil
}

func (s *memoryHistoryStore) RemoveHistory(channel string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, channel)
	return nil
}

func (s *memoryHistoryStore) Save(channel string, pub *centrifuge.Publication) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[channel]++
	nextOffset := s.offsets[channel]
	pub.Offset = uint64(nextOffset)
	s.data[channel] = append(s.data[channel], pub)
	return nextOffset
}

// SaveWithOffset saves a publication with a specific offset (from BatchIncrby).
func (s *memoryHistoryStore) SaveWithOffset(channel string, pub *centrifuge.Publication, offset uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pub.Offset = uint64(offset)
	s.data[channel] = append(s.data[channel], pub)
	s.offsets[channel] = offset
}

// redisChecker validates Redis state at key operation checkpoints.
type redisChecker struct {
	client rueidis.Client
	prefix string
}

func newRedisChecker(addr string, prefix string) (*redisChecker, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		SelectDB:    1,
	})
	if err != nil {
		return nil, err
	}
	return &redisChecker{client: client, prefix: prefix}, nil
}

func (rc *redisChecker) close() {
	rc.client.Close()
}

func (rc *redisChecker) checkMetaExists(ctx context.Context, channel string) {
	metaKey := rc.prefix + ":meta:" + channel
	exists, _ := rc.client.Do(ctx, rc.client.B().Exists().Key(metaKey).Build()).AsInt64()
	if exists > 0 {
		val, _ := rc.client.Do(ctx, rc.client.B().Hmget().Key(metaKey).Field("s", "e").Build()).AsStrSlice()
		log.Printf("  [redis-check] meta %s: exists, offset=%s epoch=%s", metaKey, val[0], val[1])
	} else {
		log.Printf("  [redis-check] meta %s: NOT FOUND", metaKey)
	}
}

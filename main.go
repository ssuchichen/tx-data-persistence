package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// Trade 表示交易数据
type Trade struct {
	ID        int
	Price     float64
	Quantity  int
	Timestamp time.Time
}

// PersistenceManager 管理交易数据持久化
type PersistenceManager struct {
	masterDB       *sql.DB
	slaveDB        *sql.DB
	redis          *redis.Client
	tradeChan      chan Trade
	buffer         []Trade
	bufferSize     int
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	maxRetries     int           // 最大重试次数
	retryDelay     time.Duration // 重试间隔
	batchCount     int64         // 批量插入次数
	batchSuccess   int64         // 批量插入成功次数
	cacheHits      int64         // 缓存命中次数
	cacheMisses    int64         // 缓存未命中次数
	redisWriteChan chan Trade    // 异步 Redis 写入通道
}

// NewPersistenceManager 初始化持久化管理器
func NewPersistenceManager(bufferSize int) *PersistenceManager {
	masterDB, err := sql.Open("mysql", "user:password@/exchange?parseTime=true")
	if err != nil {
		log.Fatal("Master DB error:", err)
	}
	slaveDB, err := sql.Open("mysql", "user:password@/exchange?parseTime=true")
	if err != nil {
		log.Fatal("Slave DB error:", err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx, cancel := context.WithCancel(context.Background())
	pm := &PersistenceManager{
		masterDB:       masterDB,
		slaveDB:        slaveDB,
		redis:          rdb,
		tradeChan:      make(chan Trade, 1000),
		buffer:         make([]Trade, 0, bufferSize),
		bufferSize:     bufferSize,
		ctx:            ctx,
		cancel:         cancel,
		maxRetries:     3,
		retryDelay:     1 * time.Second,
		redisWriteChan: make(chan Trade, 1000),
	}
	go pm.persistTrades()
	go pm.asyncRedisWriter()
	go pm.monitor()
	return pm
}

// persistTrades 处理交易持久化
func (pm *PersistenceManager) persistTrades() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			pm.flushBuffer()
			log.Println("Persistence Goroutine stopped")
			return
		case trade := <-pm.tradeChan:
			pm.mu.Lock()
			pm.buffer = append(pm.buffer, trade)
			if len(pm.buffer) >= pm.bufferSize {
				pm.flushBuffer()
			}
			pm.mu.Unlock()
			pm.redisWriteChan <- trade // 异步写入 Redis
		case <-ticker.C:
			pm.mu.Lock()
			if len(pm.buffer) > 0 {
				pm.flushBuffer()
			}
			pm.mu.Unlock()
		}
	}
}

// flushBuffer 批量插入缓冲区数据
func (pm *PersistenceManager) flushBuffer() {
	if len(pm.buffer) == 0 {
		return
	}

	pm.batchCount++
	var placeholders []string
	var args []interface{}
	for _, trade := range pm.buffer {
		placeholders = append(placeholders, "(?, ?, ?, ?)")
		args = append(args, trade.ID, trade.Price, trade.Quantity, trade.Timestamp)
	}
	query := fmt.Sprintf("INSERT INTO trades (id, price, quantity, timestamp) VALUES %s",
		strings.Join(placeholders, ","))

	for i := 0; i < pm.maxRetries; i++ {
		_, err := pm.masterDB.ExecContext(pm.ctx, query, args...)
		if err == nil {
			pm.batchSuccess++
			pm.buffer = pm.buffer[:0]
			return
		}
		log.Printf("Batch insert attempt %d failed: %v", i+1, err)
		time.Sleep(pm.retryDelay)
	}
	log.Println("Batch insert failed after retries")
}

// asyncRedisWriter 异步写入 Redis
func (pm *PersistenceManager) asyncRedisWriter() {
	for {
		select {
		case <-pm.ctx.Done():
			log.Println("Redis writer Goroutine stopped")
			return
		case trade := <-pm.redisWriteChan:
			pm.cacheTrade(trade)
		}
	}
}

// cacheTrade 将交易缓存到 Redis
func (pm *PersistenceManager) cacheTrade(trade Trade) {
	data, err := json.Marshal(trade)
	if err != nil {
		log.Println("Marshal error:", err)
		return
	}
	key := fmt.Sprintf("trade:%d", trade.ID)
	for i := 0; i < pm.maxRetries; i++ {
		err = pm.redis.Set(pm.ctx, key, data, 1*time.Hour).Err()
		if err == nil {
			return
		}
		log.Printf("Redis cache attempt %d failed: %v", i+1, err)
		time.Sleep(pm.retryDelay)
	}
	log.Printf("Redis cache failed for trade %d after retries", trade.ID)
}

// GetTrade 从 Redis 或从库读取交易
func (pm *PersistenceManager) GetTrade(id int) (*Trade, error) {
	key := fmt.Sprintf("trade:%d", id)

	// 先查 Redis
	if data, err := pm.redis.Get(pm.ctx, key).Bytes(); err == nil {
		var trade Trade
		if err := json.Unmarshal(data, &trade); err == nil {
			pm.cacheHits++
			return &trade, nil
		}
	}
	pm.cacheMisses++

	// 从从库查询
	var trade Trade
	for i := 0; i < pm.maxRetries; i++ {
		err := pm.slaveDB.QueryRowContext(pm.ctx, "SELECT id, price, quantity, timestamp FROM trades WHERE id = ?", id).
			Scan(&trade.ID, &trade.Price, &trade.Quantity, &trade.Timestamp)
		if err == nil {
			pm.cacheTrade(trade) // 异步缓存
			return &trade, nil
		}
		log.Printf("DB query attempt %d failed: %v", i+1, err)
		time.Sleep(pm.retryDelay)
	}
	return nil, fmt.Errorf("failed to get trade %d after retries", id)
}

// monitor 监控批量插入和缓存命中率
func (pm *PersistenceManager) monitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			log.Println("Monitor Goroutine stopped")
			return
		case <-ticker.C:
			pm.mu.Lock()
			total := pm.cacheHits + pm.cacheMisses
			hitRate := 0.0
			if total > 0 {
				hitRate = float64(pm.cacheHits) / float64(total) * 100
			}
			log.Printf("Batch: %d total, %d success | Cache: %.2f%% hit rate (%d hits, %d misses)",
				pm.batchCount, pm.batchSuccess, hitRate, pm.cacheHits, pm.cacheMisses)
			pm.mu.Unlock()
		}
	}
}

// Stop 停止持久化管理器
func (pm *PersistenceManager) Stop() {
	pm.cancel()
	pm.masterDB.Close()
	pm.slaveDB.Close()
	pm.redis.Close()
}

func main() {
	pm := NewPersistenceManager(5)

	// 模拟交易数据
	go func() {
		for i := 1; i <= 10; i++ {
			pm.tradeChan <- Trade{ID: i, Price: 100.0 + float64(i), Quantity: 10, Timestamp: time.Now()}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 模拟查询
	time.Sleep(2 * time.Second)
	trade, err := pm.GetTrade(3)
	if err != nil {
		fmt.Println("Get trade error:", err)
	} else {
		fmt.Printf("Retrieved trade: %+v\n", trade)
	}

	// 停止
	time.Sleep(5 * time.Second)
	pm.Stop()
}

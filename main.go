package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "sync"
    "time"
)

type HistoryEntry struct {
    RequestID string        `json:"requestId"`
    Op        string        `json:"op"`       // GET or PUT
    Key       string        `json:"key"`
    Value     string        `json:"value,omitempty"`
    Result    string        `json:"result"`   // "ok", "duplicate", "not_found"
    Start     time.Time     `json:"start"`
    End       time.Time     `json:"end"`
    Duration  time.Duration `json:"duration"`
}

type Store struct {
    mu        sync.Mutex
    kv        map[string]string
    seenWrite map[string]struct{} // requestID is set for idempotent PUT reqs
    history   []HistoryEntry
}

func NewStore() *Store {
    return &Store{
        kv:        make(map[string]string),
        seenWrite: make(map[string]struct{}),
        history:   make([]HistoryEntry, 0, 1024),
    }
}

func (s *Store) recordHistory(h HistoryEntry) {
    s.history = append(s.history, h)
}

type putRequest struct {
    RequestID string `json:"requestId"`
    Key       string `json:"key"`
    Value     string `json:"value"`
}

type getResponse struct {
    Key    string `json:"key"`
    Value  string `json:"value,omitempty"`
    Found  bool   `json:"found"`
    Result string `json:"result"`
}

func (s *Store) handlePut(w http.ResponseWriter, r *http.Request) {
    start := time.Now()
    var req putRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }
    if req.RequestID == "" || req.Key == "" {
        http.Error(w, "requestId and key required", http.StatusBadRequest)
        return
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    result := "ok"
    if _, exists := s.seenWrite[req.RequestID]; exists {
        result = "duplicate"
    } else {
        s.kv[req.Key] = req.Value
        s.seenWrite[req.RequestID] = struct{}{}
    }

    end := time.Now()
    s.recordHistory(HistoryEntry{
        RequestID: req.RequestID,
        Op:        "PUT",
        Key:       req.Key,
        Value:     req.Value,
        Result:    result,
        Start:     start,
        End:       end,
        Duration:  end.Sub(start),
    })

    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(map[string]string{"result": result})
}

func (s *Store) handleGet(w http.ResponseWriter, r *http.Request) {
    start := time.Now()
    key := r.URL.Query().Get("key")
    reqID := r.Header.Get("X-Request-ID") 
    if key == "" {
        http.Error(w, "key required", http.StatusBadRequest)
        return
    }

    s.mu.Lock()
    value, ok := s.kv[key]
    end := time.Now()
    s.recordHistory(HistoryEntry{
        RequestID: reqID,
        Op:        "GET",
        Key:       key,
        Value:     value,
        Result:    map[bool]string{true: "ok", false: "not_found"}[ok],
        Start:     start,
        End:       end,
        Duration:  end.Sub(start),
    })
    s.mu.Unlock()

    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(getResponse{
        Key:    key,
        Value:  value,
        Found:  ok,
        Result: map[bool]string{true: "ok", false: "not_found"}[ok],
    })
}

func (s *Store) handleHistory(w http.ResponseWriter, r *http.Request) {
    s.mu.Lock()
    historyCopy := make([]HistoryEntry, len(s.history))
    copy(historyCopy, s.history)
    s.mu.Unlock()

    w.Header().Set("Content-Type", "application/json")
    enc := json.NewEncoder(w)
    enc.SetIndent("", "  ")
    _ = enc.Encode(historyCopy)
}

func main() {
    s := NewStore()
    mux := http.NewServeMux()
    mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
        switch r.Method {
        case http.MethodPut, http.MethodPost:
            s.handlePut(w, r)
        case http.MethodGet:
            s.handleGet(w, r)
        default:
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        }
    })
    mux.HandleFunc("/history", s.handleHistory)

    addr := ":8080"
    log.Printf("linear-kv listening on %s", addr)
    srv := &http.Server{
        Addr:              addr,
        Handler:           loggingMiddleware(mux),
        ReadHeaderTimeout: 5 * time.Second,
    }
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatalf("server error: %v", err)
    }
}

func loggingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        next.ServeHTTP(w, r)
        dur := time.Since(start)
        fmt.Printf("%s %s %s %v\n", r.RemoteAddr, r.Method, r.URL.String(), dur)
    })
}



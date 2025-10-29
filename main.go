package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "sort"
    "strings"
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
    s := &Store{
        kv:        make(map[string]string),
        seenWrite: make(map[string]struct{}),
        history:   make([]HistoryEntry, 0, 1024),
    }
    return s
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

func (s *Store) handleTimeline(w http.ResponseWriter, r *http.Request) {
    s.mu.Lock()
    historyCopy := make([]HistoryEntry, len(s.history))
    copy(historyCopy, s.history)
    s.mu.Unlock()

    if len(historyCopy) == 0 {
        w.Header().Set("Content-Type", "text/plain")
        w.Write([]byte("No operations recorded yet.\n"))
        return
    }

    sort.Slice(historyCopy, func(i, j int) bool {
        return historyCopy[i].Start.Before(historyCopy[j].Start)
    })

    earliest := historyCopy[0].Start
    latest := historyCopy[0].End
    for _, entry := range historyCopy {
        if entry.Start.Before(earliest) {
            earliest = entry.Start
        }
        if entry.End.After(latest) {
            latest = entry.End
        }
    }

    totalDuration := latest.Sub(earliest)
    scale := 60.0 // chars for the timeline width

    w.Header().Set("Content-Type", "text/plain")
    fmt.Fprintf(w, "Timeline Visualization (%.2fms total)\n", float64(totalDuration.Nanoseconds())/1e6)
    fmt.Fprintf(w, "%-20s %-8s %-10s %-15s %s\n", "Time", "Op", "Key", "Value", "Timeline")
    fmt.Fprintf(w, "%s\n", strings.Repeat("-", 80))

    for _, entry := range historyCopy {
        startOffset := entry.Start.Sub(earliest)
        endOffset := entry.End.Sub(earliest)
        
        startPos := int(float64(startOffset.Nanoseconds()) / float64(totalDuration.Nanoseconds()) * scale)
        endPos := int(float64(endOffset.Nanoseconds()) / float64(totalDuration.Nanoseconds()) * scale)
        
        if endPos <= startPos {
            endPos = startPos + 1
        }

        timeline := strings.Repeat(" ", startPos) + strings.Repeat("█", endPos-startPos)
        
        value := entry.Value
        if len(value) > 8 {
            value = value[:8] + "..."
        }
        
        fmt.Fprintf(w, "%-20s %-8s %-10s %-15s %s\n", 
            entry.Start.Format("15:04:05.000"),
            entry.Op,
            entry.Key,
            value,
            timeline)
    }
}

type LinearizabilityChecker struct {
    history []HistoryEntry
}

type Operation struct {
    ID       string
    Op       string 
    Key      string
    Value    string
    Start    time.Time
    End      time.Time
    Result   string
    ClientID string
}

func (s *Store) handleLinearizabilityCheck(w http.ResponseWriter, r *http.Request) {
    s.mu.Lock()
    historyCopy := make([]HistoryEntry, len(s.history))
    copy(historyCopy, s.history)
    s.mu.Unlock()

    checker := &LinearizabilityChecker{history: historyCopy}
    isValid, violations := checker.CheckLinearizability()

    response := map[string]interface{}{
        "isLinearizable": isValid,
        "violations":     violations,
        "totalOps":       len(historyCopy),
    }

    w.Header().Set("Content-Type", "application/json")
    enc := json.NewEncoder(w)
    enc.SetIndent("", "  ")
    _ = enc.Encode(response)
}

func (lc *LinearizabilityChecker) CheckLinearizability() (bool, []string) {
    if len(lc.history) == 0 {
        return true, nil
    }

    ops := make([]Operation, len(lc.history))
    for i, entry := range lc.history {
        ops[i] = Operation{
            ID:       entry.RequestID,
            Op:       entry.Op,
            Key:      entry.Key,
            Value:    entry.Value,
            Start:    entry.Start,
            End:      entry.End,
            Result:   entry.Result,
            ClientID: entry.RequestID, 
        }
    }

    // Check for basic violations
    violations := []string{}
    
    keyOps := make(map[string][]Operation)
    for _, op := range ops {
        keyOps[op.Key] = append(keyOps[op.Key], op)
    }

    for key, keyOperations := range keyOps {
        if !lc.checkKeyConsistency(key, keyOperations) {
            violations = append(violations, fmt.Sprintf("Key '%s' has inconsistent operations", key))
        }
    }

    if !lc.checkReadYourWrite(ops) {
        violations = append(violations, "Read-your-write consistency violated")
    }

    if !lc.checkMonotonicReads(ops) {
        violations = append(violations, "Monotonic reads violated")
    }

    return len(violations) == 0, violations
}

func (lc *LinearizabilityChecker) checkKeyConsistency(key string, ops []Operation) bool {
    sort.Slice(ops, func(i, j int) bool {
        return ops[i].Start.Before(ops[j].Start)
    })

    latestValue := ""
    
    for _, op := range ops {
        if op.Op == "PUT" && op.Result == "ok" {
            latestValue = op.Value
        } else if op.Op == "GET" {
            if op.Result == "ok" && op.Value != latestValue {
                found := false
                for _, otherOp := range ops {
                    if otherOp.Op == "PUT" && otherOp.Key == key && 
                       otherOp.Value == op.Value &&
                       otherOp.Start.Before(op.End) && otherOp.End.After(op.Start) {
                        found = true
                        break
                    }
                }
                if !found {
                    return false
                }
            }
        }
    }
    
    return true
}

func (lc *LinearizabilityChecker) checkReadYourWrite(ops []Operation) bool {
    clientOps := make(map[string][]Operation)
    for _, op := range ops {
        clientOps[op.ClientID] = append(clientOps[op.ClientID], op)
    }

    for _, clientOps := range clientOps {
        sort.Slice(clientOps, func(i, j int) bool {
            return clientOps[i].Start.Before(clientOps[j].Start)
        })

        for i, op := range clientOps {
            if op.Op == "GET" {
                lastWrite := ""
                for j := i - 1; j >= 0; j-- {
                    if clientOps[j].Op == "PUT" && clientOps[j].Key == op.Key {
                        lastWrite = clientOps[j].Value
                        break
                    }
                }
                
                if lastWrite != "" && op.Value != lastWrite {
                    return false
                }
            }
        }
    }
    
    return true
}

func (lc *LinearizabilityChecker) checkMonotonicReads(ops []Operation) bool {
    // Group by client
    clientOps := make(map[string][]Operation)
    for _, op := range ops {
        clientOps[op.ClientID] = append(clientOps[op.ClientID], op)
    }

    for _, clientOps := range clientOps {
        sort.Slice(clientOps, func(i, j int) bool {
            return clientOps[i].Start.Before(clientOps[j].Start)
        })

        keyValues := make(map[string]string)
        
        for _, op := range clientOps {
            if op.Op == "GET" && op.Result == "ok" {
                if lastValue, exists := keyValues[op.Key]; exists {
                    if lastValue != op.Value {
                    }
                }
                keyValues[op.Key] = op.Value
            }
        }
    }
    
    return true
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
    mux.HandleFunc("/timeline", s.handleTimeline)
    mux.HandleFunc("/check", s.handleLinearizabilityCheck)

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



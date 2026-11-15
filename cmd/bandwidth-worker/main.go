// Command bandwidth-worker sets per-torrent upload/download rate limits via fleet.torrents.setRates.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type settings struct {
	DefaultUpKiBps   int64 `json:"defaultUpKiBps"`
	DefaultDownKiBps int64 `json:"defaultDownKiBps"`
}

type hostReply struct {
	result json.RawMessage
	err    string
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := append([]byte(nil), in.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.bandwidth.getSettings":
		st, err := loadSettings()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.bandwidth.saveSettings":
		var st settings
		if err := json.Unmarshal(req.Params, &st); err != nil {
			replyErr(req.ID, "invalid settings")
			return
		}
		st = normalizeSettings(st)
		if err := saveSettings(st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.bandwidth.apply":
		var p struct {
			Hashes      []string `json:"hashes"`
			UpKiBps     *int64   `json:"upKiBps"`
			DownKiBps   *int64   `json:"downKiBps"`
			ClientID    string   `json:"clientId"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			replyErr(req.ID, "invalid params")
			return
		}
		st, _ := loadSettings()
		upKiB := st.DefaultUpKiBps
		downKiB := st.DefaultDownKiBps
		if p.UpKiBps != nil {
			upKiB = *p.UpKiBps
		}
		if p.DownKiBps != nil {
			downKiB = *p.DownKiBps
		}
		if upKiB < 0 {
			upKiB = 0
		}
		if downKiB < 0 {
			downKiB = 0
		}
		res, err := applyRates(p.Hashes, p.ClientID, upKiB*1024, downKiB*1024)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.bandwidth.clear":
		var p struct {
			Hashes   []string `json:"hashes"`
			ClientID string   `json:"clientId"`
		}
		_ = json.Unmarshal(req.Params, &p)
		res, err := applyRates(p.Hashes, p.ClientID, 0, 0)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func applyRates(hashes []string, clientID string, upBps, downBps int64) (map[string]any, error) {
	clean := make([]string, 0, len(hashes))
	for _, h := range hashes {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		clean = append(clean, h)
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("hashes required")
	}
	raw, err := hostCall("fleet.torrents.setRates", map[string]any{
		"hashes":   clean,
		"clientId": strings.TrimSpace(clientID),
		"upRate":   upBps,
		"downRate": downBps,
	})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{"ok": true, "applied": len(clean), "upRate": upBps, "downRate": downBps}, nil
	}
	out["upKiBps"] = upBps / 1024
	out["downKiBps"] = downBps / 1024
	return out, nil
}

func normalizeSettings(st settings) settings {
	if st.DefaultUpKiBps < 0 {
		st.DefaultUpKiBps = 0
	}
	if st.DefaultDownKiBps < 0 {
		st.DefaultDownKiBps = 0
	}
	return st
}

func loadSettings() (settings, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "settings"})
	if err != nil {
		return settings{}, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return settings{}, nil
	}
	var st settings
	_ = json.Unmarshal(raw, &st)
	return normalizeSettings(st), nil
}

func saveSettings(st settings) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "settings", "value": normalizeSettings(st)})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

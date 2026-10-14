package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

func collectUDPCounters(scenario string) ([]measurement, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://127.0.0.1:19090/debug/vars")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("UDP 计数 HTTP %d", response.StatusCode)
	}
	var result struct {
		Stats map[string]map[string]map[string]int64 `json:"stats"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	counters, found := result.Stats["dataplane"]["udp_drop"]
	if !found {
		return nil, fmt.Errorf("节点没有 UDP 计数器族")
	}
	var rows []measurement
	for _, layer := range []string{"pipe", "hub", "dispatcher", "wireguard", "hysteria"} {
		value, found := counters[layer]
		if !found {
			return nil, fmt.Errorf("节点没有 %s UDP 计数器", layer)
		}
		rows = append(rows, scalar(scenario+"_drop_"+layer, "packets", float64(value)))
	}
	return rows, nil
}

func startClientProfiles(directory string) (func() error, error) {
	if directory == "" {
		return func() error { return nil }, nil
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}
	file, err := os.Create(filepath.Join(directory, "client-cpu.pprof"))
	if err != nil {
		return nil, err
	}
	runtime.SetMutexProfileFraction(10)
	runtime.SetBlockProfileRate(1000000)
	if err := pprof.StartCPUProfile(file); err != nil {
		file.Close()
		return nil, err
	}
	return func() error {
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			return err
		}
		for _, name := range []string{"mutex", "block"} {
			output, err := os.Create(filepath.Join(directory, "client-"+name+".pprof"))
			if err != nil {
				return err
			}
			writeError := pprof.Lookup(name).WriteTo(output, 0)
			closeError := output.Close()
			if writeError != nil {
				return writeError
			}
			if closeError != nil {
				return closeError
			}
		}
		return nil
	}, nil
}

func collectNodeProfiles(options options) error {
	if err := os.MkdirAll(options.profiles, 0755); err != nil {
		return err
	}
	client := &http.Client{Timeout: time.Duration(options.seconds+10) * time.Second}
	for _, name := range []string{"cpu", "mutex", "block"} {
		endpoint := name
		if name == "cpu" {
			endpoint = fmt.Sprintf("profile?seconds=%d", options.seconds)
		}
		response, err := client.Get("http://127.0.0.1:19090/debug/pprof/" + endpoint)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return fmt.Errorf("node %s profile: HTTP %d", name, response.StatusCode)
		}
		output, err := os.Create(filepath.Join(options.profiles, "node-"+name+".pprof"))
		if err != nil {
			response.Body.Close()
			return err
		}
		_, copyError := io.Copy(output, response.Body)
		response.Body.Close()
		closeError := output.Close()
		if copyError != nil {
			return copyError
		}
		if closeError != nil {
			return closeError
		}
	}
	return nil
}

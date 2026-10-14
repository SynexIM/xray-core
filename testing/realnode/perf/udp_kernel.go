package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func udpKernelCounters() (map[string]uint64, error) {
	values := make(map[string]uint64)
	for _, namespace := range []string{"perf-client", "perf-node", "perf-target"} {
		data, err := exec.Command("ip", "netns", "exec", namespace, "cat", "/proc/net/snmp").Output()
		if err != nil {
			return nil, err
		}
		var names []string
		for _, line := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(line, "Udp:") {
				continue
			}
			fields := strings.Fields(line)
			if names == nil {
				names = fields
				continue
			}
			if len(fields) != len(names) {
				return nil, fmt.Errorf("%s UDP 内核计数格式不匹配", namespace)
			}
			for index := 1; index < len(names); index++ {
				value, err := strconv.ParseUint(fields[index], 10, 64)
				if err != nil {
					return nil, err
				}
				values[namespace+"_"+names[index]] = value
			}
		}
	}
	return values, nil
}

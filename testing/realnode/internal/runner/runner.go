package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type Child struct {
	Command *exec.Cmd
	Log     string
	done    chan error
}

func Start(ctx context.Context, binary, directory, name string, config any, address string, timeout time.Duration) (*Child, error) {
	return StartInNamespace(ctx, binary, directory, name, config, address, timeout, "")
}

func StartInNamespace(ctx context.Context, binary, directory, name string, config any, address string, timeout time.Duration, namespace string) (*Child, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, name+".json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return nil, err
	}
	logPath := filepath.Join(directory, name+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, binary, "run", "-c", path)
	if namespace != "" {
		command = exec.CommandContext(ctx, "ip", "netns", "exec", namespace, binary, "run", "-c", path)
	}
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	child := &Child{Command: command, Log: logPath, done: make(chan error, 1)}
	go func() {
		waitError := command.Wait()
		closeError := logFile.Close()
		if waitError == nil {
			waitError = closeError
		}
		child.done <- waitError
	}()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case processError := <-child.done:
			logs, readError := os.ReadFile(logPath)
			if readError != nil {
				return nil, readError
			}
			return nil, fmt.Errorf("%s 启动失败: %v\n%s", name, processError, logs)
		default:
		}
		if ready(ctx, namespace, address) {
			return child, nil
		}
		select {
		case <-ctx.Done():
			child.Stop()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	child.Stop()
	logs, err := os.ReadFile(logPath)
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%s 未在 %s 内就绪\n%s", name, timeout, logs)
}

func ready(ctx context.Context, namespace, address string) bool {
	if namespace != "" {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return false
		}
		return exec.CommandContext(ctx, "ip", "netns", "exec", namespace, "nc", "-z", "-w", "1", host, port).Run() == nil
	}
	connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err != nil {
		return false
	}
	connection.Close()
	return true
}

func (child *Child) Stop() {
	child.Command.Process.Signal(os.Interrupt)
	select {
	case <-child.done:
	case <-time.After(2 * time.Second):
		child.Command.Process.Kill()
		<-child.done
	}
}

func FreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

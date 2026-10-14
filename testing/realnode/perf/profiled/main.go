// 与正式二进制加载相同模块，只在隔离基准进程开启锁和阻塞采样。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/xtls/xray-core/core"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintln(os.Stderr, "仅供基准：run -c config.json")
		os.Exit(1)
	}
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	path := flags.String("c", "", "完整启动配置")
	flags.Parse(os.Args[2:])
	data, err := os.ReadFile(*path)
	if err != nil {
		panic(err)
	}
	runtime.SetMutexProfileFraction(10)
	runtime.SetBlockProfileRate(1000000)
	instance, err := core.StartInstance("json", data)
	if err != nil {
		panic(err)
	}
	defer instance.Close()
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt, syscall.SIGTERM)
	<-stopped
}

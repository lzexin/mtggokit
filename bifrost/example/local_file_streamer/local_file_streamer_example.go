package main

import (
	"context"
	"fmt"
	"github.com/lzexin/mtggokit/bifrost/container"
	"github.com/lzexin/mtggokit/bifrost/streamer"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
)

func main() {
	lfs := streamer.NewFileStreamer(&streamer.LocalFileStreamerCfg{
		Name:       "campaign_list.txt",
		Path:       "bifrost/data/campaign_list_bug.txt",
		UpdatMode:  streamer.Dynamic,
		Interval:   5,
		IsSync:     true,
		DataParser: &streamer.DefaultTextParser{},
		Logger:     logrus.New(),
	})
	lfs.SetContainer(&container.BufferedMapContainer{
		Tolerate: 0.001,
	})
	if lfs == nil {
		fmt.Println("Init local file streamer error!")
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan os.Signal)
	signal.Notify(c)
	_ = lfs.UpdateData(ctx)

	value, err := lfs.GetContainer().Get(container.StrKey("CampaignList"))
	if err == nil {
		//fmt.Println(value)
	}
	value, err = lfs.GetContainer().Get(container.StrKey("2_TV"))
	if err == nil {
		fmt.Println(value)
	}

	s := <-c
	fmt.Println("退出信号", s)
}

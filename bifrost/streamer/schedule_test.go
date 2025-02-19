package streamer

import (
	"context"
	"fmt"
	"github.com/lzexin/mtggokit/bifrost/container"
	"github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

type FakeSchedStreamer struct {
	Name      string
	SchedInfo *SchedInfo
}

func (*FakeSchedStreamer) SetContainer(container.Container) {

}

func (*FakeSchedStreamer) GetContainer() container.Container {
	return nil
}

func (fs *FakeSchedStreamer) UpdateData(ctx context.Context) error {
	fmt.Printf("Name: %s, interval[%d], timestamp[%d]\n", fs.Name, fs.SchedInfo.TimeInterval, time.Now().Unix())
	return nil
}

func (fs *FakeSchedStreamer) GetSchedInfo() *SchedInfo {
	return fs.SchedInfo
}

func (fs *FakeSchedStreamer) GetInfo() *Info {
	return nil
}

func TestSched_Schedule(t *testing.T) {
	convey.Convey("Test schedule", t, func() {
		sched := Sched{}
		sched.AddStreamer("test1", &FakeSchedStreamer{
			Name:      "fake1",
			SchedInfo: &SchedInfo{TimeInterval: 1},
		})
		sched.AddStreamer("test3", &FakeSchedStreamer{
			Name:      "fake3",
			SchedInfo: &SchedInfo{TimeInterval: 3},
		})
		sched.AddStreamer("test7", &FakeSchedStreamer{
			Name:      "fake7",
			SchedInfo: &SchedInfo{TimeInterval: 7},
		})
		sched.AddStreamer("test5", &FakeSchedStreamer{
			Name:      "fake5",
			SchedInfo: &SchedInfo{TimeInterval: 5},
		})

		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()
		sched.Schedule(ctx)
	})
}

func TestSelect(t *testing.T) {
	inc := time.After(time.Second)
	base := time.After(time.Second * 5)
	for i := 0; i < 10; i++ {
		select {
		case <-inc:
			fmt.Println("inc:", i)
			inc = time.After(time.Second)
		case <-base:
			fmt.Println("base: ", i)
			time.Sleep(time.Second * 2)
			base = time.After(time.Second * 3)
		}

	}
}

package container

import (
	"errors"
	"github.com/smartystreets/goconvey/convey"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBlockingMapContainer_LoadBase(t *testing.T) {
	convey.Convey("Test BufferedMapContainer Get", t, func() {
		bm := CreateBlockingMapContainer(1, 0)
		convey.So(bm.LoadBase(NewTestDataIter([]string{})), convey.ShouldBeNil)
		convey.So(bm.errorNum, convey.ShouldEqual, 0)
		convey.So(bm.Len(), convey.ShouldEqual, 0)
	})

	convey.Convey("Test BufferedMapContainer Get", t, func() {
		bm := CreateBlockingMapContainer(1, 0)
		convey.So(bm.LoadBase(NewTestDataIter([]string{
			"1\t2",
			"a\tb",
		})), convey.ShouldBeNil)
		convey.So(bm.errorNum, convey.ShouldEqual, 0)
		convey.So(bm.InnerData, convey.ShouldNotBeNil)

		convey.So(bm.Len(), convey.ShouldEqual, 2)

		v, e := bm.Get(StrKey("1"))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "2")

		v, e = bm.Get(StrKey("a"))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "b")
	})

	convey.Convey("Test BufferedMapContainer Get", t, func() {
		bm := CreateBlockingMapContainer(1, 0)
		convey.So(bm.LoadBase(NewTestIntDataIter([]string{
			"1\t2",
			"4\tb",
		})), convey.ShouldBeNil)
		convey.So(bm.errorNum, convey.ShouldEqual, 0)
		convey.So(bm.Len(), convey.ShouldEqual, 2)
		v, e := bm.Get(I64Key(1))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "2")

		v, e = bm.Get(I64Key(4))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "b")
	})

	convey.Convey("Test BufferedMapContainer Get", t, func() {
		bm := CreateBlockingMapContainer(1, 0)
		convey.So(bm.LoadBase(NewTestIntDataIter([]string{
			"1\t2",
			"4\tb",
			"2",
		})), convey.ShouldNotBeNil)
		convey.So(bm.errorNum, convey.ShouldEqual, 1)
		convey.So(bm.totalNum, convey.ShouldEqual, 3)
		convey.So(bm.Len(), convey.ShouldEqual, 0)

	})

	convey.Convey("Test BufferedMapContainer Get", t, func() {
		bm := CreateBlockingMapContainer(1, 0.5)
		convey.So(bm.LoadBase(NewTestIntDataIter([]string{
			"1\t2",
			"4\tb",
			"2",
		})), convey.ShouldBeNil)
		convey.So(bm.errorNum, convey.ShouldEqual, 1)
		convey.So(bm.totalNum, convey.ShouldEqual, 3)
		convey.So(bm.Len(), convey.ShouldEqual, 2)

		v, e := bm.Get(I64Key(1))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "2")

		v, e = bm.Get(I64Key(4))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "b")
	})

}

func TestBlockingMapContainer_LoadInc(t *testing.T) {
	convey.Convey("Test BufferedMapContainer Get", t, func() {
		bm := CreateBlockingMapContainer(1, 0.5)
		convey.So(bm.LoadBase(NewTestIntDataIter([]string{
			"1\t2",
			"4\tb",
			"2",
		})), convey.ShouldBeNil)
		convey.So(bm.errorNum, convey.ShouldEqual, 1)
		convey.So(bm.totalNum, convey.ShouldEqual, 3)

		convey.So(bm.Len(), convey.ShouldEqual, 2)

		v, e := bm.Get(I64Key(1))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "2")

		v, e = bm.Get(I64Key(4))
		convey.So(e, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, "b")

		convey.Convey("Test LoadIncSucc", func() {
			convey.So(bm.LoadInc(NewTestIntDataIter([]string{
				"5\t3",
				"2",
			})), convey.ShouldBeNil)
			convey.So(bm.errorNum, convey.ShouldEqual, 2)
			convey.So(bm.totalNum, convey.ShouldEqual, 5)
			convey.So(bm.Len(), convey.ShouldEqual, 3)

		})

		convey.Convey("Test LoadInc Fail", func() {
			convey.So(bm.LoadInc(NewTestIntDataIter([]string{
				"5",
				"2",
			})), convey.ShouldNotBeNil)
			convey.So(bm.errorNum, convey.ShouldEqual, 3)
			convey.So(bm.totalNum, convey.ShouldEqual, 5)
			convey.So(bm.Len(), convey.ShouldEqual, 2)
		})
	})
}

func TestBlockingMapContainer_LoadInc2(t *testing.T) {
	bm := CreateBlockingMapContainer(0, 0)
	for i := 0; i < 10; i ++ {
		go func() {
			for i:= 0; i< 1000; i++ {
				_ = bm.LoadInc(NewTestIntDataIter2([]string{
					"1\t\t0",
					"2\t3\t1",
					"3\t3\t2",
					"4\t3\t1",
					"5\t3\t2",
					"6\t3\t1",
					"7\t3\t2",
					"8\t3\t0",
					"9\t3\t1",
					"10\t3\t2",
				}))
			}
		}()
		go func() {
			for i := 0; i < 10; i ++ {
				_, _ = bm.Get(I64Key(int64(i)))
			}
		}()
	}
	time.Sleep(1000 * time.Second)
}

func NewTestIntDataIter2(data []string) *TestIntDataIter2 {
	return &TestIntDataIter2{
		current: 0,
		data:    data,
	}
}

type TestIntDataIter2 struct {
	current int
	data    []string
}

func (this *TestIntDataIter2) HasNext() (bool, error) {
	return this.current < len(this.data), nil
}

func (this *TestIntDataIter2) Next() (DataMode, MapKey, interface{}, error) {
	defer func() { this.current++ }()
	if this.current >= len(this.data) {
		return DataModeAdd, nil, nil, errors.New("current index is error")
	}
	s := this.data[this.current]
	items := strings.SplitN(s, "\t", 3)
	if len(items) != 3 {
		return DataModeAdd, nil, nil, errors.New("items len is not 3, item[" + s + "]")
	}
	n, e := strconv.ParseInt(items[0], 10, 64)
	if e != nil {
		return DataModeAdd, nil, nil, errors.New("parse key error, not an number")
	}
	if items[2] == "2" {
		return DataModeDel, I64Key(n), items[1], nil
	}
	return DataModeAdd, I64Key(n), items[1], nil
}
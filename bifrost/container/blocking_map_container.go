package container

import (
	"errors"
	"fmt"
)

// 多线程读写安全的container，支持增量
type BlockingMapContainer struct {
	InnerData *ConcurrentSliceMap
	errorNum  int64
	totalNum  int64
	Tolerate  float64
}

func CreateBlockingMapContainer(numPartision int, tolerate float64) *BlockingMapContainer {
	return &BlockingMapContainer{
		InnerData: CreateConcurrentSliceMap(10000),
		Tolerate:  tolerate,
	}
}

func (bm *BlockingMapContainer) Get(key MapKey) (interface{}, error) {
	if bm.InnerData == nil {
		return nil, NotExistErr
	}
	data, in := bm.InnerData.Load(key.Value())
	if !in {
		return nil, NotExistErr
	}
	return data, nil
}

func (bm *BlockingMapContainer) Set(key MapKey, value interface{}) error {
	bm.InnerData.Store(key.Value(), value)
	return nil
}

func (bm *BlockingMapContainer) Del(key MapKey, value interface{}) {
	bm.InnerData.Delete(key.Value())
}

func (bm *BlockingMapContainer) LoadBase(iterator DataIterator) error {
	bm.errorNum = 0
	bm.totalNum = 0

	b, e := iterator.HasNext()
	if e != nil {
		return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
	}
	for b {
		m, k, v, e := iterator.Next()
		bm.totalNum++
		if e != nil {
			bm.errorNum++
			b, e = iterator.HasNext()
			if e != nil {
				return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
			}
			continue
		}
		switch m {
		case DataModeAdd, DataModeUpdate:
			bm.InnerData.Store(k.Value(), v)
		case DataModeDel:
			bm.InnerData.Delete(k.Value())
		}
		b, e = iterator.HasNext()
		if e != nil {
			return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
		}
	}
	if bm.totalNum == 0 {
		bm.totalNum = 1
	}
	f := float64(bm.errorNum) / float64(bm.totalNum)
	if f > bm.Tolerate {
		bm.InnerData = &ConcurrentSliceMap{}
		return errors.New(fmt.Sprintf("LoadBase error, tolerate[%f], err[%f]", bm.Tolerate, f))
	}
	return nil
}

func (bm *BlockingMapContainer) LoadInc(iterator DataIterator) error {
	b, e := iterator.HasNext()
	if e != nil {
		return fmt.Errorf("LoadInc Error, err[%s]", e.Error())
	}
	for b {
		m, k, v, e := iterator.Next()
		bm.totalNum++
		if e != nil {
			bm.errorNum++
			b, e = iterator.HasNext()
			if e != nil {
				return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
			}
			continue
		}
		switch m {
		case DataModeAdd, DataModeUpdate:
			bm.InnerData.Store(k.Value(), v)
		case DataModeDel:
			bm.Del(k, v)
		}
		b, e = iterator.HasNext()
		if e != nil {
			return fmt.Errorf("LoadInc Error, err[%s]", e.Error())
		}
	}
	if bm.totalNum == 0 {
		bm.totalNum = 1
	}
	f := float64(bm.errorNum) / float64(bm.totalNum)
	if f > bm.Tolerate {
		return errors.New(fmt.Sprintf("LoadInc error, tolerate[%f], err[%f]", bm.Tolerate, f))
	}
	return nil
}

func (bm *BlockingMapContainer) Len() int {
	return bm.InnerData.Len()
}

func (bm *BlockingMapContainer) Range(f func(key, value interface{}) bool) {
	bm.InnerData.Range(f)
}

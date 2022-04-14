package container

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

type ConcurrentSliceMap struct {
	totalNum    int                 // 当前所存数据量级，新数据下标
	index       map[interface{}]int // 维护数据在slice中的下标

	partitions []*innerSlice // 分桶slice
	mu         sync.RWMutex   // 读写锁 - 扩容partitions时需要加锁

	lenOfBucket int // 桶容积
}

type innerSlice struct {
	s []unsafe.Pointer
}

// CreateConcurrentSliceMap is to create a ConcurrentSliceMap with the setting number of the partitions & len of the Buckets
// NumOfPartitions will auto add capacity
func CreateConcurrentSliceMap(lenOfBucket int) *ConcurrentSliceMap {
	return &ConcurrentSliceMap{
		totalNum:    0,
		index:       map[interface{}]int{},
		partitions:  []*innerSlice{},
		lenOfBucket: lenOfBucket,
	}
}

func createInnerMap(lenOfBuckets int) *innerSlice {
	return &innerSlice{
		s: make([]unsafe.Pointer, lenOfBuckets),
	}
}

var NotPartition = errors.New("not partition")
var expunged = unsafe.Pointer(new(interface{}))

func (m *ConcurrentSliceMap) getPartitionWithIndex(key interface{}) (partition int, index int, err error) {
	if m.index == nil || m.lenOfBucket == 0 {
		err = NotPartition
		return
	}

	n, in := m.index[key] // 获取key对应的下标(>=0)
	if !in {
		err = NotPartition
		return
	}

	p := n / m.lenOfBucket
	if p > len(m.partitions)-1 {
		err = NotPartition
		return
	}

	partition = p
	index = n % m.lenOfBucket

	return partition, index, err
}

func (m *ConcurrentSliceMap) Len() int {
	return m.totalNum
}

func (m *ConcurrentSliceMap) Load(key interface{}) (interface{}, bool) {
	m.mu.RLock()

	partition, index, err := m.getPartitionWithIndex(key)
	if err != nil || m.partitions[partition].s[index] == nil {
		m.mu.RUnlock()
		return nil, false
	}

	p := atomic.LoadPointer(&m.partitions[partition].s[index])
	if p == nil || p == expunged {
		m.mu.RUnlock()
		return nil, false
	}

	m.mu.RUnlock()

	return *(*interface{})(p), true
}

func (m *ConcurrentSliceMap) Store(key interface{}, v interface{}) {
	m.mu.Lock()
	// 1. 判断该key是否已记录下标，若有直接替换
	if p, i, e := m.getPartitionWithIndex(key); e == nil {
		m.partitions[p].s[i] = unsafe.Pointer(&v)
		m.mu.Unlock()
		return
	}

	// 2. 若没有往后增加
	partition := m.totalNum / m.lenOfBucket //新数据的桶
	index := m.totalNum % m.lenOfBucket     //新数据的下标

	if partition >= len(m.partitions) {
		m.partitions = append(m.partitions, createInnerMap(m.lenOfBucket))
	}
	m.partitions[partition].s[index] = unsafe.Pointer(&v)
	m.index[key] = m.totalNum
	m.totalNum++
	m.mu.Unlock()
}

func (m *ConcurrentSliceMap) Delete(key interface{}) {
	m.mu.RLock()
	p, i, e := m.getPartitionWithIndex(key)
	if e == nil && m.partitions[p].s[i] != nil {
		m.partitions[p].s[i] = nil
	}
	m.mu.RUnlock()
}

func (m *ConcurrentSliceMap) Range(f func(key, value interface{}) bool) {
	return
}

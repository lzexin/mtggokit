package container

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

// todo 先确认功能能跑通，再优化部分性能
// 1.delete、update操作map下标不删除，只置空slice。
// 2.slice不能用append，需要提前初始化，或者分桶append。
// 3.map+mutex对比 sync.map性能，读>写时sync.map更快
// 4.slice 线程不安全问题
// 5. （mutex + slice） + map 对比  sync.map+(slice+mutex)
// 6. 分段锁
// 在查找元素上，最慢的是原生 map+互斥锁，其次是原生 map+读写锁。最快的是 sync.map 类型。
// 在写入元素上，最慢的是 sync.map 类型，其次是原生 map+互斥锁（Mutex），最快的是原生 map+读写锁（RwMutex）。
// 在删除元素上，最慢的是原生 map+读写锁，其次是原生 map+互斥锁，最快的是 sync.map 类型。

type ConcurrentSliceMap struct {
	totalNum int       // 当前所存数据量级，新数据下标
	index    *sync.Map // 维护数据在slice中的下标

	mu         sync.RWMutex  // 读写锁 - 扩容partitions时需要加锁
	partitions []*innerSlice // 分桶slice

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
		index:       &sync.Map{},
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

	n, in := m.index.Load(key) // 获取key对应的下标(>=0)
	if !in {
		err = NotPartition
		return
	}

	p := n.(int) / m.lenOfBucket
	if p > len(m.partitions)-1 {
		err = NotPartition
		return
	}

	partition = p
	index = n.(int) % m.lenOfBucket

	return partition, index, err
}

func (m *ConcurrentSliceMap) Len() int {
	return m.totalNum
}

func (m *ConcurrentSliceMap) Load(key interface{}) (interface{}, bool) {
	partition, index, err := m.getPartitionWithIndex(key)
	if err != nil || m.partitions[partition].s[index] == nil {
		return nil, false
	}

	p := atomic.LoadPointer(&m.partitions[partition].s[index])
	if p == nil || p == expunged {
		return nil, false
	}

	return *(*interface{})(p), true
}

func (m *ConcurrentSliceMap) Store(key interface{}, v interface{}) {
	// 1. 判断该key是否已记录下标，若有直接替换
	if p, i, e := m.getPartitionWithIndex(key); e == nil {
		m.partitions[p].s[i] = unsafe.Pointer(&v)
		return
	}

	// 2. 若没有往后增加
	partition := m.totalNum / m.lenOfBucket //新数据的桶
	index := m.totalNum % m.lenOfBucket     //新数据的下标

	m.mu.Lock()
	if partition >= len(m.partitions) {
		m.partitions = append(m.partitions, createInnerMap(m.lenOfBucket))
	}
	m.mu.Unlock()

	m.partitions[partition].s[index] = unsafe.Pointer(&v)
	m.index.Store(key, m.totalNum)
	m.totalNum++
}

func (m *ConcurrentSliceMap) Delete(key interface{}) {
	p, i, e := m.getPartitionWithIndex(key)
	if e == nil {
		m.partitions[p].s[i] = nil
	}
}

func (m *ConcurrentSliceMap) Range(f func(key, value interface{}) bool) {
	if m.index != nil {
		m.index.Range(f)
	}
}

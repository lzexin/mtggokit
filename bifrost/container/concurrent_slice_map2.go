package container

import (
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

type ConcurrentSliceMap2 struct {
	totalNum    int                 // 当前所存数据量级，新数据下标
	index       map[interface{}]int // 维护数据在slice中的下标
	inactiveNum int                 // 已失效数量

	partitions []*innerSlice2 // 分桶slice
	mu         sync.RWMutex   // 读写锁 - 扩容partitions时需要加锁

	lenOfBucket int // 桶容积
}

type innerSlice2 struct {
	s []unsafe.Pointer
}

// CreateConcurrentSliceMap2 is to create a ConcurrentSliceMap2 with the setting number of the partitions & len of the Buckets
// NumOfPartitions will auto add capacity
func CreateConcurrentSliceMap2(lenOfBucket int) *ConcurrentSliceMap2 {
	return &ConcurrentSliceMap2{
		totalNum:    0,
		index:       map[interface{}]int{},
		partitions:  []*innerSlice2{},
		lenOfBucket: lenOfBucket,
	}
}

func createInnerMap2(lenOfBuckets int) *innerSlice2 {
	return &innerSlice2{
		s: make([]unsafe.Pointer, lenOfBuckets),
	}
}

func (m *ConcurrentSliceMap2) getPartitionWithIndex(key interface{}) (partition int, index int, err error) {
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

func (m *ConcurrentSliceMap2) Len() int {
	return m.totalNum - m.inactiveNum
}

func (m *ConcurrentSliceMap2) Load(key interface{}) (interface{}, bool) {
	m.mu.RLock()

	partition, index, err := m.getPartitionWithIndex(key)
	if err != nil || m.partitions[partition].s[index] == nil {
		return nil, false
	}

	p := atomic.LoadPointer(&m.partitions[partition].s[index])
	if p == nil || p == expunged {
		return nil, false
	}

	m.mu.RUnlock()

	return *(*interface{})(p), true
}

func (m *ConcurrentSliceMap2) Store(key interface{}, v interface{}) {
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
		m.partitions = append(m.partitions, createInnerMap2(m.lenOfBucket))
	}
	m.partitions[partition].s[index] = unsafe.Pointer(&v)
	m.index[key] = m.totalNum
	m.totalNum++

	m.mu.Unlock()
}

func (m *ConcurrentSliceMap2) Delete(key interface{}) {
	m.mu.Lock()
	p, i, e := m.getPartitionWithIndex(key)
	if e == nil {
		m.partitions[p].s[i] = nil
		m.inactiveNum++
	}
	m.mu.Unlock()
}

func (m *ConcurrentSliceMap2) Range(f func(key, value interface{}) bool) {
	return
}

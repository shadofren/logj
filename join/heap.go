package join

import (
	"container/heap"
	"time"
)

type Line struct {
	ts   time.Time
	line string
	file string
}

// A Heap implements heap.Interface and holds Items.
type Heap []*Line

func (pq Heap) Len() int { return len(pq) }

func (pq Heap) Less(i, j int) bool {
	return pq[i].ts.Before(pq[j].ts)
}

func (pq Heap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *Heap) Push(x interface{}) {
	item := x.(*Line)
	*pq = append(*pq, item)
}

func (pq *Heap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func NewHeap() *Heap {

	pq := make(Heap, 0)
	heap.Init(&pq)
	return &pq
}

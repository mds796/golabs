package simplepb

// A PriorityQueue implements heap.Interface and holds PrepareArgs.
type PrepareArgsQueue []*PrepareArgs

func (pq PrepareArgsQueue) Len() int {
	return len(pq)
}

func (pq PrepareArgsQueue) Less(i, j int) bool {
	return pq[i].Index < pq[j].Index
}

func (pq PrepareArgsQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PrepareArgsQueue) Push(x interface{}) {
	n := len(*pq)
	*pq = append(*pq, x.(*PrepareArgs))
}

func (pq *PrepareArgsQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PrepareArgsQueue) Peek() *PrepareArgs {
	return &pq[0]
}

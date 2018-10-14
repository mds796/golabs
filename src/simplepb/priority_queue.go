package simplepb

// OperationsQueue is a PriorityQueue that implements heap.Interface and holds *PrepareArgs.
type OperationsQueue []*PrepareArgs

// The length of the queue
func (pq OperationsQueue) Len() int {
	return len(pq)
}

// True if i is less than j
func (pq OperationsQueue) Less(i, j int) bool {
	return pq[i].Index < pq[j].Index
}

// Swap the positions of i and j
func (pq OperationsQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push x unto the queue
func (pq *OperationsQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*PrepareArgs))
}

// Pop the next operation to be committed from the queue
func (pq *OperationsQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// Peek at the next operation to be committed
func (pq *OperationsQueue) Peek() *PrepareArgs {
	return (*pq)[0]
}

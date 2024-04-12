package inmemory

import "go-se-project/graph"

var _ graph.LinkIterator = (*iterator[graph.Link])(nil)
var _ graph.EdgeIterator = (*iterator[graph.Edge])(nil)

type iterator[T any] struct {
	g        *Graph
	items    []*T
	curIndex int
}

func (i *iterator[T]) Next() bool {
	if i.curIndex >= len(i.items) {
		return false
	}
	i.curIndex++
	return true
}

func (i *iterator[T]) Item() *T {
	i.g.mu.RLock()
	item := new(T)
	*item = *i.items[i.curIndex-1]
	i.g.mu.RUnlock()
	return item
}

func (i *iterator[T]) Error() error { return nil }

func (i *iterator[T]) Close() error { return nil }

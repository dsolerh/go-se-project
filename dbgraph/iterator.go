package dbgraph

import (
	"database/sql"
	"fmt"
	"go-se-project/graph"
)

var (
	_ graph.LinkIterator = (*linkIterator)(nil)
	_ graph.EdgeIterator = (*edgeIterator)(nil)
)

// i *linkIterator graph.LinkIterator
type linkIterator struct {
	rows        *sql.Rows
	lastErr     error
	latchedLink *graph.Link
}

func (i *linkIterator) Next() bool {
	if i.lastErr != nil || !i.rows.Next() {
		return false
	}

	l := new(graph.Link)
	i.lastErr = i.rows.Scan(&l.Id, &l.Url, &l.RetrievedAt)
	if i.lastErr != nil {
		return false
	}
	l.RetrievedAt = l.RetrievedAt.UTC()

	i.latchedLink = l
	return true
}

func (i *linkIterator) Item() *graph.Link {
	return i.latchedLink
}

func (i *linkIterator) Error() error { return i.lastErr }

func (i *linkIterator) Close() error {
	err := i.rows.Close()
	if err != nil {
		return fmt.Errorf("link iterator: %w", err)
	}

	return nil
}

// i *edgeIterator graph.EdgeIterator
type edgeIterator struct {
	rows        *sql.Rows
	lastErr     error
	latchedEdge *graph.Edge
}

func (i *edgeIterator) Next() bool {
	if i.lastErr != nil || !i.rows.Next() {
		return false
	}

	e := new(graph.Edge)
	i.lastErr = i.rows.Scan(&e.Id, &e.Src, &e.Dst, &e.UpdatedAt)
	if i.lastErr != nil {
		return false
	}
	e.UpdatedAt = e.UpdatedAt.UTC()

	i.latchedEdge = e
	return true
}

func (i *edgeIterator) Item() *graph.Edge {
	return i.latchedEdge
}

func (i *edgeIterator) Error() error { return i.lastErr }

func (i *edgeIterator) Close() error {
	err := i.rows.Close()
	if err != nil {
		return fmt.Errorf("link iterator: %w", err)
	}

	return nil
}

package graph

import (
	"time"

	"github.com/google/uuid"
)

type Graph interface {
	// links
	UpsertLink(link *Link) error
	FindLink(id uuid.UUID) (*Link, error)
	Links(fromId, toId uuid.UUID, retrieveBefore time.Time) (LinkIterator, error)
	// edges
	UpsertEdge(edge *Edge) error
	RemoveStaleEdges(fromId uuid.UUID, updatedBefore time.Time) error
	Edges(fromId, toId uuid.UUID, updatedBefore time.Time) (EdgeIterator, error)
}

type Link struct {
	Id          uuid.UUID
	Url         string
	RetrievedAt time.Time
}

type Edge struct {
	Id        uuid.UUID
	Src, Dst  uuid.UUID
	UpdatedAt time.Time
}

type Iterator[T any] interface {
	// gets the item of the iterator
	Item() T
	// Next advances the iterator. If no more items are available or an
	// error occurs, calls to Next() return false.
	Next() bool
	// Error returns the last error encountered by the iterator.
	Error() error
	// Close releases any resources associated with an iterator.
	Close() error
}

type LinkIterator = Iterator[*Link]

type EdgeIterator = Iterator[*Edge]

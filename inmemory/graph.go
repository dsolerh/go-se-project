package inmemory

import (
	"fmt"
	"sync"
	"time"

	"go-se-project/graph"

	"github.com/google/uuid"
)

type edgeList []uuid.UUID

var _ graph.Graph = (*Graph)(nil)

// g *Graph graph.Graph
type Graph struct {
	links map[uuid.UUID]*graph.Link
	edges map[uuid.UUID]*graph.Edge

	linkURLIndex map[string]*graph.Link
	linkEdgeMap  map[uuid.UUID]edgeList

	mu sync.RWMutex
}

// NewInMemoryGraph creates a new in-memory link graph.
func NewInMemoryGraph() *Graph {
	return &Graph{
		links:        make(map[uuid.UUID]*graph.Link),
		edges:        make(map[uuid.UUID]*graph.Edge),
		linkURLIndex: make(map[string]*graph.Link),
		linkEdgeMap:  make(map[uuid.UUID]edgeList),
	}
}

// links
func (g *Graph) UpsertLink(link *graph.Link) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Check if a link with the same URL already exists. If so, convert
	// this into an update and point the link ID to the existing link.
	if existing := g.linkURLIndex[link.Url]; existing != nil {
		link.Id = existing.Id
		originalTs := existing.RetrievedAt
		*existing = *link
		// check if there's a updated timestamp
		if originalTs.After(existing.RetrievedAt) {
			existing.RetrievedAt = originalTs
		}
		return nil
	}

	// Assign new ID and insert link
	for {
		link.Id = uuid.New()
		if g.links[link.Id] == nil {
			break
		}
	}

	// create a copy of the link to avoid sharing the same pointer
	lCopy := new(graph.Link)
	*lCopy = *link
	// save the link in the index by url
	g.linkURLIndex[lCopy.Url] = lCopy
	// save the link
	g.links[lCopy.Id] = lCopy

	return nil
}

func (g *Graph) FindLink(id uuid.UUID) (*graph.Link, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	link := g.links[id]
	if link == nil {
		return nil, fmt.Errorf("find link: %w", graph.ErrNotFound)
	}

	lCopy := new(graph.Link)
	*lCopy = *link
	return lCopy, nil
}

func (g *Graph) Links(fromId uuid.UUID, toId uuid.UUID, retrieveBefore time.Time) (graph.LinkIterator, error) {
	from, to := fromId.String(), toId.String()

	g.mu.RLock()
	var list []*graph.Link
	for linkId, link := range g.links {
		if id := linkId.String(); id >= from && id < to && link.RetrievedAt.Before(retrieveBefore) {
			list = append(list, link)
		}
	}
	g.mu.RUnlock()

	return &iterator[graph.Link]{g: g, items: list}, nil
}

// edges
func (g *Graph) UpsertEdge(edge *graph.Edge) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	_, srcExist := g.links[edge.Src]
	_, dstExist := g.links[edge.Dst]
	if !srcExist || !dstExist {
		return fmt.Errorf("upsert edge: %w", graph.ErrUnknownEdgeLinks)
	}

	// Scan edge list from source
	for _, edgeId := range g.linkEdgeMap[edge.Src] {
		existingEdge := g.edges[edgeId]

		if existingEdge.Src == edge.Src && existingEdge.Dst == edge.Dst {
			existingEdge.UpdatedAt = time.Now()
			*edge = *existingEdge
			return nil
		}
	}

	for {
		edge.Id = uuid.New()
		if g.edges[edge.Id] == nil {
			break
		}
	}

	edge.UpdatedAt = time.Now()
	eCopy := new(graph.Edge)
	*eCopy = *edge
	g.edges[eCopy.Id] = eCopy

	// Append the edge ID to the list of edges originating from the edge's source link.
	g.linkEdgeMap[edge.Src] = append(g.linkEdgeMap[edge.Src], eCopy.Id)
	return nil
}

func (g *Graph) RemoveStaleEdges(fromId uuid.UUID, updatedBefore time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	var newEdgeList edgeList
	for _, edgeId := range g.linkEdgeMap[fromId] {
		edge := g.edges[edgeId]
		if edge.UpdatedAt.Before(updatedBefore) {
			delete(g.edges, edgeId)
			continue
		}
		newEdgeList = append(newEdgeList, edgeId)
	}
	g.linkEdgeMap[fromId] = newEdgeList

	return nil
}

func (g *Graph) Edges(fromId uuid.UUID, toId uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	from, to := fromId.String(), toId.String()

	g.mu.RLock()
	var list []*graph.Edge
	for linkId := range g.links {
		if id := linkId.String(); id < from || id >= to {
			continue
		}
		for _, edgeID := range g.linkEdgeMap[linkId] {
			if edge := g.edges[edgeID]; edge.UpdatedAt.Before(updatedBefore) {
				list = append(list, edge)
			}
		}
	}
	g.mu.RUnlock()

	return &iterator[graph.Edge]{g: g, items: list}, nil
}

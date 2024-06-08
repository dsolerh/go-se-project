package dbgraph

import (
	"database/sql"
	"fmt"
	"go-se-project/graph"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

var (
	upsertLinkQuery = `
INSERT INTO links (url, retrieved_at) VALUES ($1, $2) 
ON CONFLICT (url) DO UPDATE SET retrieved_at=GREATEST(links.retrieved_at, $2)
RETURNING id, retrieved_at
`
	findLinkQuery         = "SELECT url, retrieved_at FROM links WHERE id=$1"
	linksInPartitionQuery = "SELECT id, url, retrieved_at FROM links WHERE id >= $1 AND id < $2 AND retrieved_at < $3"

	upsertEdgeQuery = `
INSERT INTO edges (src, dst, updated_at) VALUES ($1, $2, NOW())
ON CONFLICT (src,dst) DO UPDATE SET updated_at=NOW()
RETURNING id, updated_at
`
	edgesInPartitionQuery = "SELECT id, src, dst, updated_at FROM edges WHERE src >= $1 AND src < $2 AND updated_at < $3"
	removeStaleEdgesQuery = "DELETE FROM edges WHERE src=$1 AND updated_at < $2"

	// Compile-time check for ensuring CockroachDbGraph implements Graph.
	_ graph.Graph = (*CockroachDBGraph)(nil)
)

// g *CockroachDBGraph graph.Graph
type CockroachDBGraph struct {
	db *sql.DB
}

func NewCockroachDbGraph(dsn string) (*CockroachDBGraph, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &CockroachDBGraph{db: db}, nil
}

// links
func (g *CockroachDBGraph) UpsertLink(link *graph.Link) error {
	row := g.db.QueryRow(upsertLinkQuery, link.Url, link.RetrievedAt.UTC())
	if err := row.Scan(&link.Id, &link.RetrievedAt); err != nil {
		return fmt.Errorf("upsert link: %w", err)
	}
	link.RetrievedAt = link.RetrievedAt.UTC()
	return nil
}

func (g *CockroachDBGraph) FindLink(id uuid.UUID) (*graph.Link, error) {
	row := g.db.QueryRow(findLinkQuery, id)
	link := &graph.Link{Id: id}
	if err := row.Scan(&link.Url, &link.RetrievedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("find link: %w", graph.ErrNotFound)
		}
		return nil, fmt.Errorf("find link: %w", err)
	}
	link.RetrievedAt = link.RetrievedAt.UTC()
	return link, nil
}

func (g *CockroachDBGraph) Links(fromId uuid.UUID, toId uuid.UUID, retrieveBefore time.Time) (graph.LinkIterator, error) {
	rows, err := g.db.Query(linksInPartitionQuery, fromId, toId, retrieveBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("links: %w", err)
	}
	return &linkIterator{rows: rows}, nil
}

// edges
func (g *CockroachDBGraph) UpsertEdge(edge *graph.Edge) error {
	row := g.db.QueryRow(upsertEdgeQuery, edge.Src, edge.Dst)
	if err := row.Scan(&edge.Id, &edge.UpdatedAt); err != nil {
		if isForeignKeyViolationError(err) {
			err = graph.ErrUnknownEdgeLinks
		}
		return fmt.Errorf("upsert edge: %w", err)
	}
	edge.UpdatedAt = edge.UpdatedAt.UTC()
	return nil
}

func (g *CockroachDBGraph) RemoveStaleEdges(fromId uuid.UUID, updatedBefore time.Time) error {
	_, err := g.db.Exec(removeStaleEdgesQuery, fromId, updatedBefore.UTC())
	if err != nil {
		return fmt.Errorf("remove stale edges: %w", err)
	}
	return nil
}

func (g *CockroachDBGraph) Edges(fromId uuid.UUID, toId uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	rows, err := g.db.Query(edgesInPartitionQuery, fromId, toId,
		updatedBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("edges: %w", err)
	}
	return &edgeIterator{rows: rows}, nil
}

func isForeignKeyViolationError(err error) bool {
	pqErr, valid := err.(*pq.Error)
	if !valid {
		return false
	}
	return pqErr.Code.Name() == "foreign_key_violation"
}

package inmemory

import (
	"go-se-project/graphtest"
	"testing"
)

func TestInMemoryGraph_UpsertLink(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestUpsertLink(t)
}

func TestInMemoryGraph_FindLink(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestFindLink(t)
}

func TestInMemoryGraph_ConcurrentLinkIterators(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestConcurrentLinkIterators(t)
}

func TestInMemoryGraph_LinkIteratorTimeFilter(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestLinkIteratorTimeFilter(t)
}

func TestInMemoryGraph_PartitionedLinkIterators(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestPartitionedLinkIterators(t)
}

func TestInMemoryGraph_UpsertEdge(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestUpsertEdge(t)
}

func TestInMemoryGraph_ConcurrentEdgeIterators(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestConcurrentEdgeIterators(t)
}

func TestInMemoryGraph_EdgeIteratorTimeFilter(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestEdgeIteratorTimeFilter(t)
}

func TestInMemoryGraph_PartitionedEdgeIterators(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestPartitionedEdgeIterators(t)
}

func TestInMemoryGraph_RemoveStaleEdges(t *testing.T) {
	suite := new(graphtest.Suite)
	suite.SetGraph(NewInMemoryGraph())
	suite.TestRemoveStaleEdges(t)
}

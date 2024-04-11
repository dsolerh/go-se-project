package graphtest

import (
	"fmt"
	"go-se-project/graph"
	"math/big"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Suite defines a re-usable set of graph-related tests that can
// be executed against any type that implements graph.Graph.
type Suite struct {
	g graph.Graph
}

// SetGraph configures the test-suite to run all tests against g.
func (s *Suite) SetGraph(g graph.Graph) {
	s.g = g
}

// TestUpsertLink verifies the link upsert logic.
func (s *Suite) TestUpsertLink(t *testing.T) {
	assert := assert.New(t)

	// Create a new link
	original := &graph.Link{
		Url:         "https://example.com",
		RetrievedAt: time.Now().Add(-10 * time.Hour),
	}

	// add the link to the graph
	err := s.g.UpsertLink(original)
	// should not produce an error
	assert.Nil(err)
	// the link id should not be Nil
	assert.NotEqual(original.Id, uuid.Nil, "expected a linkID to be assigned to the new link")

	// Update existing link with a newer timestamp and different URL
	accessedAt := time.Now().Truncate(time.Second).UTC()
	existing := &graph.Link{
		Id:          original.Id,
		Url:         "https://example.com",
		RetrievedAt: accessedAt,
	}
	err = s.g.UpsertLink(existing)

	// should not produce an error
	assert.Nil(err)
	// should not change the link id
	assert.Equal(existing.Id, original.Id, "link ID changed while upserting")

	stored, err := s.g.FindLink(existing.Id)
	// should not produce an error
	assert.Nil(err)
	// should update the last access timestamp
	assert.Equal(stored.RetrievedAt, accessedAt, "last accessed timestamp was not updated")

	// Attempt to insert a new link whose URL matches an existing link with
	// and provide an older accessedAt value
	sameURL := &graph.Link{
		Url:         existing.Url,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = s.g.UpsertLink(sameURL)
	// should not produce an error
	assert.Nil(err)
	// should not create a new link but update the existing one
	assert.Equal(sameURL.Id, existing.Id)

	stored, err = s.g.FindLink(existing.Id)
	// should not produce an error
	assert.Nil(err)
	// should not override the timestamp
	assert.Equal(stored.RetrievedAt, accessedAt, "last accessed timestamp was overwritten with an older value")

	// Create a new link and then attempt to update it.
	dup := &graph.Link{
		Url: "foo",
	}
	err = s.g.UpsertLink(dup)
	// should not produce an error
	assert.Nil(err)
	// the link id should not be Nil
	assert.NotEqual(original.Id, uuid.Nil, "expected a linkID to be assigned to the new link")
}

// TestFindLink verifies the link lookup logic.
func (s *Suite) TestFindLink(t *testing.T) {
	assert := assert.New(t)

	// Create a new link
	link := &graph.Link{
		Url:         "https://example.com",
		RetrievedAt: time.Now().Truncate(time.Second).UTC(),
	}
	err := s.g.UpsertLink(link)
	// should not produce an error
	assert.Nil(err)
	// the link id should not be Nil
	assert.NotEqual(link.Id, uuid.Nil, "expected a linkID to be assigned to the new link")

	// Lookup link by ID
	other, err := s.g.FindLink(link.Id)
	// should not produce an error
	assert.Nil(err)
	// should return the same value
	assert.Equal(other, link, "lookup by ID returned the wrong link")

	// Lookup link by unknown ID
	_, err = s.g.FindLink(uuid.Nil)
	// should return an error because the link could not be found
	assert.ErrorIs(err, graph.ErrNotFound)
}

// TestConcurrentLinkIterators verifies that multiple clients can concurrently
// access the store.
func (s *Suite) TestConcurrentLinkIterators(t *testing.T) {
	assert := assert.New(t)

	var (
		wg           sync.WaitGroup
		numIterators = 10
		numLinks     = 100
	)

	for i := 0; i < numLinks; i++ {
		link := &graph.Link{Url: fmt.Sprint(i)}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
	}

	wg.Add(numIterators)

	for i := 0; i < numIterators; i++ {
		go func(id int) {
			defer wg.Done()

			seen := make(map[string]bool)
			it, err := s.partitionedLinkIterator(assert, 0, 1, time.Now())
			// should not produce an error
			assert.Nil(err, "err: %v iterator %d", err, id)

			defer func() {
				err := it.Close()
				assert.Nil(err, "err: %v iterator %d", err, id)
			}()

			for i := 0; it.Next(); i++ {
				link := it.Item()
				linkID := link.Id.String()

				assert.False(seen[linkID], "iterator %d saw same link twice", id)
				seen[linkID] = true
			}

			assert.Len(seen, numLinks, "iterator %d", id)
			assert.Nil(it.Error(), "iterator %d", id)
			assert.Nil(it.Close(), "iterator %d", id)
		}(i)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	// test completed successfully
	case <-time.After(10 * time.Second):
		assert.Fail("timed out waiting for test to complete")
	}
}

// TestLinkIteratorTimeFilter verifies that the time-based filtering of the
// link iterator works as expected.
func (s *Suite) TestLinkIteratorTimeFilter(t *testing.T) {
	assert := assert.New(t)

	linkUUIDs := make([]uuid.UUID, 3)
	linkInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		link := &graph.Link{Url: fmt.Sprint(i), RetrievedAt: time.Now()}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
		linkUUIDs[i] = link.Id
		linkInsertTimes[i] = time.Now()
	}

	for i, updateTime := range linkInsertTimes {
		t.Logf("fetching links created before edge %d", i)
		s.assertIteratedLinkIDsMatch(assert, updateTime, linkUUIDs[:i+1])
	}
}

// TestPartitionedLinkIterators verifies that the graph partitioning logic
// works as expected even when partitions contain an uneven number of items.
func (s *Suite) TestPartitionedLinkIterators(t *testing.T) {
	assert := assert.New(t)

	numLinks := 100
	numPartitions := 10
	for i := 0; i < numLinks; i++ {
		err := s.g.UpsertLink(&graph.Link{Url: fmt.Sprint(i)})
		// should not produce an error
		assert.Nil(err)
	}

	// Check with both odd and even partition counts to check for rounding-related bugs.
	num := s.iteratePartitionedLinks(assert, numPartitions)
	assert.Equal(num, numLinks)
	s.iteratePartitionedLinks(assert, numPartitions+1)
	assert.Equal(num, numLinks)
}

// TestUpsertEdge verifies the edge upsert logic.
func (s *Suite) TestUpsertEdge(t *testing.T) {
	assert := assert.New(t)

	// Create links
	linkUUIDs := make([]uuid.UUID, 3)
	for i := 0; i < 3; i++ {
		link := &graph.Link{Url: fmt.Sprint(i)}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
		linkUUIDs[i] = link.Id
	}

	// Create a edge
	edge := &graph.Edge{
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}

	err := s.g.UpsertEdge(edge)
	// should not produce an error
	assert.Nil(err)
	// the edge should be added
	assert.NotEqual(edge.Id, uuid.Nil, "expected an edgeID to be assigned to the new edge")
	// the UpdateAt field should be set
	assert.False(edge.UpdatedAt.IsZero(), "UpdatedAt field not set")

	// Update existing edge
	other := &graph.Edge{
		Id:  edge.Id,
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}

	err = s.g.UpsertEdge(other)
	// should not produce an error
	assert.Nil(err)
	// the edge id should not be updated
	assert.Equal(other.Id, edge.Id, "edge ID changed while upserting")
	// the UpdatedAt field should be updated
	assert.NotEqual(other.UpdatedAt, edge.UpdatedAt, "UpdatedAt field not modified")

	// Create edge with unknown link IDs
	bogus := &graph.Edge{
		Src: linkUUIDs[0],
		Dst: uuid.New(),
	}
	err = s.g.UpsertEdge(bogus)
	assert.ErrorIs(err, graph.ErrUnknownEdgeLinks)
}

// TestConcurrentEdgeIterators verifies that multiple clients can concurrently
// access the store.
func (s *Suite) TestConcurrentEdgeIterators(t *testing.T) {
	assert := assert.New(t)

	var (
		wg           sync.WaitGroup
		numIterators = 10
		numEdges     = 100
		linkUUIDs    = make([]uuid.UUID, numEdges*2)
	)

	for i := 0; i < numEdges*2; i++ {
		link := &graph.Link{Url: fmt.Sprint(i)}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
		linkUUIDs[i] = link.Id
	}
	for i := 0; i < numEdges; i++ {
		err := s.g.UpsertEdge(&graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		})
		// should not produce an error
		assert.Nil(err)
	}

	wg.Add(numIterators)
	for i := 0; i < numIterators; i++ {
		go func(id int) {
			defer wg.Done()

			seen := make(map[string]bool)
			it, err := s.partitionedEdgeIterator(assert, 0, 1, time.Now())
			// should not produce an error
			assert.Nil(err, "iterator %d", id)
			defer func() {
				err := it.Close()
				// should not produce an error
				assert.Nil(err, "iterator %d", id)
			}()

			for i := 0; it.Next(); i++ {
				edge := it.Item()
				edgeID := edge.Id.String()
				// should not have seen this edge before
				assert.False(seen[edgeID], "iterator %d saw same edge twice", id)
				seen[edgeID] = true
			}

			assert.Len(seen, numEdges, "iterator %d", id)
			assert.Nil(it.Error(), "iterator %d", id)
			assert.Nil(it.Close(), "iterator %d", id)
		}(i)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	// test completed successfully
	case <-time.After(10 * time.Second):
		assert.Fail("timed out waiting for test to complete")
	}
}

// TestEdgeIteratorTimeFilter verifies that the time-based filtering of the
// edge iterator works as expected.
func (s *Suite) TestEdgeIteratorTimeFilter(t *testing.T) {
	assert := assert.New(t)

	linkUUIDs := make([]uuid.UUID, 3)
	linkInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		link := &graph.Link{Url: fmt.Sprint(i)}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
		linkUUIDs[i] = link.Id
		linkInsertTimes[i] = time.Now()
	}

	edgeUUIDs := make([]uuid.UUID, len(linkUUIDs))
	edgeInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		edge := &graph.Edge{Src: linkUUIDs[0], Dst: linkUUIDs[i]}
		err := s.g.UpsertEdge(edge)
		// should not produce an error
		assert.Nil(err)
		edgeUUIDs[i] = edge.Id
		edgeInsertTimes[i] = time.Now()
	}

	for i, updateTime := range edgeInsertTimes {
		t.Logf("fetching edges created before edge %d", i)
		s.assertIteratedEdgeIDsMatch(assert, updateTime, edgeUUIDs[:i+1])
	}
}

// TestPartitionedEdgeIterators verifies that the graph partitioning logic
// works as expected even when partitions contain an uneven number of items.
func (s *Suite) TestPartitionedEdgeIterators(t *testing.T) {
	assert := assert.New(t)

	numEdges := 100
	numPartitions := 10
	linkUUIDs := make([]uuid.UUID, numEdges*2)
	for i := 0; i < numEdges*2; i++ {
		link := &graph.Link{Url: fmt.Sprint(i)}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
		linkUUIDs[i] = link.Id
	}
	for i := 0; i < numEdges; i++ {
		err := s.g.UpsertEdge(&graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		})
		// should not produce an error
		assert.Nil(err)
	}

	// Check with both odd and even partition counts to check for rounding-related bugs.
	num := s.iteratePartitionedEdges(assert, numPartitions)
	assert.Equal(num, numEdges)
	num = s.iteratePartitionedEdges(assert, numPartitions+1)
	assert.Equal(num, numEdges)
}

// TestRemoveStaleEdges verifies that the edge deletion logic works as expected.
func (s *Suite) TestRemoveStaleEdges(t *testing.T) {
	assert := assert.New(t)

	numEdges := 100
	linkUUIDs := make([]uuid.UUID, numEdges*4)
	goneUUIDs := make(map[uuid.UUID]struct{})
	for i := 0; i < numEdges*4; i++ {
		link := &graph.Link{Url: fmt.Sprint(i)}
		err := s.g.UpsertLink(link)
		// should not produce an error
		assert.Nil(err)
		linkUUIDs[i] = link.Id
	}

	var lastTs time.Time
	for i := 0; i < numEdges; i++ {
		e1 := &graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}
		err := s.g.UpsertEdge(e1)
		// should not produce an error
		assert.Nil(err)
		goneUUIDs[e1.Id] = struct{}{}
		lastTs = e1.UpdatedAt
	}

	deleteBefore := lastTs.Add(time.Millisecond)
	time.Sleep(250 * time.Millisecond)

	// The following edges will have an updated at value > lastTs
	for i := 0; i < numEdges; i++ {
		e2 := &graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[numEdges+i+1],
		}
		err := s.g.UpsertEdge(e2)
		// should not produce an error
		assert.Nil(err)
	}
	err := s.g.RemoveStaleEdges(linkUUIDs[0], deleteBefore)
	// should not produce an error
	assert.Nil(err)

	it, err := s.partitionedEdgeIterator(assert, 0, 1, time.Now())
	// should not produce an error
	assert.Nil(err)
	defer func() { assert.Nil(it.Close()) }()

	var seen int
	for it.Next() {
		id := it.Item().Id
		_, found := goneUUIDs[id]
		assert.False(found, "expected edge %s to be removed from the edge list", id.String())
		seen++
	}

	assert.Equal(seen, numEdges)
}

/*
	Utils
*/

func (s *Suite) iteratePartitionedEdges(assert *assert.Assertions, numPartitions int) int {
	seen := make(map[string]bool)
	for partition := 0; partition < numPartitions; partition++ {
		// Build list of expected edges per partition. An edge belongs to a
		// partition if its origin link also belongs to the same partition.
		linksInPartition := make(map[uuid.UUID]struct{})
		linkIt, err := s.partitionedLinkIterator(assert, partition, numPartitions, time.Now())
		// should not produce an error
		assert.Nil(err)

		for linkIt.Next() {
			linkID := linkIt.Item().Id
			linksInPartition[linkID] = struct{}{}
		}

		// should not produce an error
		assert.Nil(linkIt.Error())
		// should not produce an error
		assert.Nil(linkIt.Close())

		it, err := s.partitionedEdgeIterator(assert, partition, numPartitions, time.Now())
		// should not produce an error
		assert.Nil(err)
		defer func() {
			// should not produce an error
			assert.Nil(it.Close())
		}()

		for it.Next() {
			edge := it.Item()
			edgeID := edge.Id.String()
			assert.False(seen[edgeID], "iterator returned same edge in different partitions")
			seen[edgeID] = true

			_, srcInPartition := linksInPartition[edge.Src]
			assert.True(srcInPartition, "iterator returned an edge whose source link belongs to a different partition")
		}

		// should not produce an error
		assert.Nil(it.Error())
		// should not produce an error
		assert.Nil(it.Close())
	}

	return len(seen)
}

func (s *Suite) assertIteratedEdgeIDsMatch(assert *assert.Assertions, updatedBefore time.Time, exp []uuid.UUID) {
	it, err := s.partitionedEdgeIterator(assert, 0, 1, updatedBefore)
	// should not produce an error
	assert.Nil(err)

	var got []uuid.UUID
	for it.Next() {
		got = append(got, it.Item().Id)
	}
	// should not produce an error
	assert.Nil(it.Error())
	// should not produce an error
	assert.Nil(it.Close())

	sort.Slice(got, func(l, r int) bool { return got[l].String() < got[r].String() })
	sort.Slice(exp, func(l, r int) bool { return exp[l].String() < exp[r].String() })
	assert.Equal(got, exp)
}

func (s *Suite) iteratePartitionedLinks(assert *assert.Assertions, numPartitions int) int {
	seen := make(map[string]bool)
	for partition := 0; partition < numPartitions; partition++ {
		it, err := s.partitionedLinkIterator(assert, partition, numPartitions, time.Now())
		// should not produce an error
		assert.Nil(err)
		defer func() {
			err := it.Close()
			// should not produce an error
			assert.Nil(err)
		}()

		for it.Next() {
			link := it.Item()
			linkID := link.Id.String()
			assert.False(seen[linkID], "iterator returned same link in different partitions")
			seen[linkID] = true
		}

		// should not produce an error
		assert.Nil(it.Error())
		// should not produce an error
		assert.Nil(it.Close())
	}

	return len(seen)
}

func (s *Suite) assertIteratedLinkIDsMatch(assert *assert.Assertions, updatedBefore time.Time, exp []uuid.UUID) {
	it, err := s.partitionedLinkIterator(assert, 0, 1, updatedBefore)
	// should not produce an error
	assert.Nil(err)

	var got []uuid.UUID
	for it.Next() {
		got = append(got, it.Item().Id)
	}
	// should not produce an error
	assert.Nil(it.Error())
	// should not produce an error
	assert.Nil(it.Close())

	sort.Slice(got, func(l, r int) bool { return got[l].String() < got[r].String() })
	sort.Slice(exp, func(l, r int) bool { return exp[l].String() < exp[r].String() })

	assert.Equal(got, exp)
}

func (s *Suite) partitionedLinkIterator(assert *assert.Assertions, partition, numPartitions int, accessedBefore time.Time) (graph.Iterator[*graph.Link], error) {
	from, to := s.partitionRange(assert, partition, numPartitions)
	return s.g.Links(from, to, accessedBefore)
}

func (s *Suite) partitionedEdgeIterator(assert *assert.Assertions, partition, numPartitions int, updatedBefore time.Time) (graph.Iterator[*graph.Edge], error) {
	from, to := s.partitionRange(assert, partition, numPartitions)
	return s.g.Edges(from, to, updatedBefore)
}

func (s *Suite) partitionRange(assert *assert.Assertions, partition, numPartitions int) (from, to uuid.UUID) {
	assert.Condition(func() bool { return partition >= 0 && partition < numPartitions }, "invalid partition")

	var minUUID = uuid.Nil
	var maxUUID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	var err error

	// Calculate the size of each partition as: (2^128 / numPartitions)
	tokenRange := big.NewInt(0)
	partSize := big.NewInt(0)
	partSize.SetBytes(maxUUID[:])
	partSize = partSize.Div(partSize, big.NewInt(int64(numPartitions)))

	// We model the partitions as a segment that begins at minUUID (all
	// bits set to zero) and ends at maxUUID (all bits set to 1). By
	// setting the end range for the *last* partition to maxUUID we ensure
	// that we always cover the full range of UUIDs even if the range
	// itself is not evenly divisible by numPartitions.
	if partition == 0 {
		from = minUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition)))
		from, err = uuid.FromBytes(tokenRange.Bytes())
		// should not produce an error
		assert.Nil(err)
	}

	if partition == numPartitions-1 {
		to = maxUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition+1)))
		to, err = uuid.FromBytes(tokenRange.Bytes())
		// should not produce an error
		assert.Nil(err)
	}

	return from, to
}

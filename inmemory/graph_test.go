package inmemory

import (
	"go-se-project/graphtest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type GraphTestSuite struct {
	graphtest.Suite
}

func (suite *GraphTestSuite) SetupTest() {
	suite.SetGraph(NewInMemoryGraph())
}

// Register our test-suite with go test.
func Test(t *testing.T) { suite.Run(t, new(GraphTestSuite)) }

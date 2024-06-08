package dbgraph

import (
	"database/sql"
	"go-se-project/graphtest"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type GraphTestSuite struct {
	graphtest.Suite
	db *sql.DB
}

func (suite *GraphTestSuite) SetupSuite() {
	dsn := os.Getenv("CDB_DSN")
	if dsn == "" {
		suite.FailNow("Missing CDB_DSN envvar; cannot run the test suite")
	}

	g, err := NewCockroachDbGraph(dsn)
	suite.Nil(err)
	suite.SetGraph(g)
	suite.db = g.db
}

func (suite *GraphTestSuite) SetupTest() {
	suite.flushDB()
}

func (suite *GraphTestSuite) TearDownTest() {
	if suite.db != nil {
		suite.flushDB()
		suite.Nil(suite.db.Close())
	}
}

// Register our test-suite with go test.
func Test(t *testing.T) { suite.Run(t, new(GraphTestSuite)) }

func (suite *GraphTestSuite) flushDB() {
	_, err := suite.db.Exec("DELETE FROM links")
	suite.Nil(err)
	_, err = suite.db.Exec("DELETE FROM edges")
	suite.Nil(err)
}

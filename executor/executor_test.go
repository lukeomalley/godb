package executor

import (
	"testing"
)

func TestScanIterator(t *testing.T) {
	tuples := buildTestTuples()

	operators :=
		[]Operator{
			{name: SCAN, parameters: []string{}},
		}

	parser := NewOperatorParser(operators)

	iterators := parser.ParseQueryPlan()

	e := New(iterators, tuples)

	result := e.Run()

	if len(result) != 4 {
		t.Fatalf("Result does not contain 3 tuples got %d.", len(result))
	}
}

func TestFilterIterator(t *testing.T) {
	tuples := buildTestTuples()

	operators :=
		[]Operator{
			{name: FILTER, parameters: []string{"name", "=", "luke"}},
			{name: SCAN, parameters: []string{}},
		}

	queryPlan := NewQueryPlan(operators)

	e := New(*queryPlan, tuples)

	result := e.Run()

	if len(result) != 1 {
		t.Fatalf("Result does not contain 1 tuples got %d.", len(result))
	}

	if result[0].Values[0].Value != "luke" {
		t.Fatalf("Result did not filter correctly. Exptecte tuple name to equal luke. got=%s", result[0].Values[0].Value)
	}

}

// ============================================================================
// Test Helpers
// ============================================================================

func buildTestTuples() []Tuple {
	return []Tuple{
		newTuple("id", "1", "name", "luke", "age", "25"),
		newTuple("id", "2", "name", "meagan", "age", "22"),
		newTuple("id", "3", "name", "elon", "age", "30"),
		newTuple("id", "4", "name", "randy", "age", "12"),
	}
}

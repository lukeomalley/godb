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

	result := execute(operators, tuples)

	if len(result) != 5 {
		t.Fatalf("Result does not contain 5 tuples got %d.", len(result))
	}
}

func TestFilterIterator(t *testing.T) {
	tests := []struct {
		operators      []Operator
		expectedTuples int
		expectedValue  string
	}{
		{
			operators: []Operator{
				{name: FILTER, parameters: []string{"name", "=", "luke"}},
				{name: SCAN, parameters: []string{}},
			},
			expectedTuples: 2,
			expectedValue:  "luke",
		},
		{
			operators: []Operator{
				{name: FILTER, parameters: []string{"name", "=", "meagan"}},
				{name: SCAN, parameters: []string{}},
			},
			expectedTuples: 1,
			expectedValue:  "meagan",
		},
		{
			operators: []Operator{
				{name: FILTER, parameters: []string{"age", "=", "12"}},
				{name: SCAN, parameters: []string{}},
			},
			expectedTuples: 1,
			expectedValue:  "randy",
		},
	}

	for _, tt := range tests {
		tuples := buildTestTuples()
		result := execute(tt.operators, tuples)

		if len(result) != tt.expectedTuples {
			t.Fatalf("Result does not contain %d tuples got %d.", tt.expectedTuples, len(result))
		}

		if result[0].Values[1].Value != tt.expectedValue {
			t.Fatalf("Result did not filter correctly. Expected tuple name to equal %s. got=%s", tt.expectedValue, result[0].Values[1].Value)
		}
	}
}

func TestLimitIterator(t *testing.T) {
	tests := []struct {
		operators      []Operator
		expectedTuples int
		expectedValue  string
	}{
		{
			operators: []Operator{
				{name: LIMIT, parameters: []string{"1"}},
				{name: SCAN, parameters: []string{}},
			},
			expectedTuples: 1,
		},
		{
			operators: []Operator{
				{name: LIMIT, parameters: []string{"2"}},
				{name: SCAN, parameters: []string{}},
			},
			expectedTuples: 2,
		},
	}

	for _, tt := range tests {
		tuples := buildTestTuples()
		result := execute(tt.operators, tuples)

		if len(result) != tt.expectedTuples {
			t.Fatalf("Result does not contain %d tuples got %d.", tt.expectedTuples, len(result))
		}
	}
}

// ============================================================================
// Test Helpers
// ============================================================================

func execute(operators []Operator, tuples []Tuple) []Tuple {
	parser := NewOperatorParser(operators, tuples)
	iterators := parser.ParseQueryPlan()
	e := NewExecutor(iterators, tuples)

	return e.Run()
}

func buildTestTuples() []Tuple {
	return []Tuple{
		newTuple("id", "1", "name", "luke", "age", "25"),
		newTuple("id", "2", "name", "meagan", "age", "22"),
		newTuple("id", "3", "name", "elon", "age", "30"),
		newTuple("id", "4", "name", "randy", "age", "12"),
		newTuple("id", "5", "name", "luke", "age", "32"),
	}
}

func newTuple(inputs ...interface{}) Tuple {
	if len(inputs)%2 != 0 {
		panic("number of inputs to newTuple must be even")
	}

	tuple := Tuple{Values: make([]Value, 0, len(inputs)/2)}

	for i := 0; i < len(inputs); i += 2 {
		tuple.Values = append(tuple.Values, Value{
			Key:         inputs[i].(string),
			Value:       inputs[i+1].(string),
			StringValue: inputs[i+1].(string),
		})
	}

	return tuple
}

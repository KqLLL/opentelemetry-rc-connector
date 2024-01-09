package sqlparse

import (
	"testing"
)

func TestExtractSQL_SelectStmt(t *testing.T) {
	sql := "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"

	expectedTables := []string{"users", "orders"}
	expectedJoinType := "LeftJoin"
	expectedOperation := "Select"

	// Call the function to test
	result, err := ExtractSQL(sql)
	if err != nil {
		t.Errorf("ExtractSQL returned an error: %v", err)
		return
	}

	// Check if the result is not nil
	if result == nil {
		t.Errorf("ExtractSQL returned nil result")
		return
	}

	// Check if the tables were parsed correctly
	if len(result.Tables) != len(expectedTables) {
		t.Errorf("Expected %d tables, got %d", len(expectedTables), len(result.Tables))
	}
	for i, table := range expectedTables {
		if result.Tables[i] != table {
			t.Errorf("Expected table %s, got %s", table, result.Tables[i])
		}
	}

	// Check if the join type was parsed correctly
	if result.JoinType != expectedJoinType {
		t.Errorf("Expected join type %s, got %s", expectedJoinType, result.JoinType)
	}

	// Check if the operation was parsed correctly
	if result.Operation != expectedOperation {
		t.Errorf("Expected operation %s, got %s", expectedOperation, result.Operation)
	}
}

func TestExtractSQL_UpdateStmt(t *testing.T) {
	sql := "UPDATE users SET name = 'John Doe' WHERE id = 42"

	expectedTable := "users"
	expectedOperation := "Update"

	// Call the function to test
	result, err := ExtractSQL(sql)
	if err != nil {
		t.Errorf("ExtractSQL returned an error: %v", err)
		return
	}

	// Check if the result is not nil
	if result == nil {
		t.Errorf("ExtractSQL returned nil result")
		return
	}

	// Check if the table was parsed correctly
	if len(result.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(result.Tables))
	} else if result.Tables[0] != expectedTable {
		t.Errorf("Expected table %s, got %s", expectedTable, result.Tables[0])
	}

	// Check if the operation was parsed correctly
	if result.Operation != expectedOperation {
		t.Errorf("Expected operation %s, got %s", expectedOperation, result.Operation)
	}
}

func TestExtractSQL_InsertStmt(t *testing.T) {
	sql := "INSERT INTO orders (user_id, status) VALUES (1, 'shipped')"

	expectedTable := "orders"
	expectedOperation := "Insert"

	// Call the function to test
	result, err := ExtractSQL(sql)
	if err != nil {
		t.Errorf("ExtractSQL returned an error: %v", err)
		return
	}

	// Check if the result is not nil
	if result == nil {
		t.Errorf("ExtractSQL returned nil result")
		return
	}

	// Check if the table was parsed correctly
	if len(result.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(result.Tables))
	} else if result.Tables[0] != expectedTable {
		t.Errorf("Expected table %s, got %s", expectedTable, result.Tables[0])
	}

	// Check if the operation was parsed correctly
	if result.Operation != expectedOperation {
		t.Errorf("Expected operation %s, got %s", expectedOperation, result.Operation)
	}
}

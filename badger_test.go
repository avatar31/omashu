package omashu

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/avatar31/omashu/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/avatar31/omashu/utils"
)

func setupMockDB(managed bool) (*Badger, func()) {
	ctx := context.Background()
	var mockDB *Badger
	if managed {
		bdb, _ := badger.OpenManaged(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
		mockDB = &Badger{db: bdb, managed: true}
	} else {
		bdb, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
		mockDB = &Badger{db: bdb, managed: false}
	}

	data, err := os.ReadFile("../../../assets/db/fixtures.json")
	if err != nil {
		panic(err)
	}

	var result map[string]any
	json.Unmarshal(data, &result)

	for key, value := range result {
		switch v := value.(type) {
		case map[string]any:
			b, _ := json.Marshal(v)
			mockDB.Set(ctx, key, b)
		case float64:
			b := utils.Uint64ToBytes(uint64(v))
			mockDB.Set(ctx, key, b)
		default:
			fmt.Printf("Unhandled type %T\n", v)
		}
	}

	return mockDB, func() {
		mockDB.db.Close()
	}
}

type bulkGetTC struct {
	name          string
	keys          []string
	expectedCount int
	expectedError bool
	db            *Badger
	validateValue func(*testing.T, map[string][]byte)
}

var bulkGetTestcases = []bulkGetTC{
	{
		name:          "BulkGet existing keys",
		keys:          []string{"users:1", "users:2", "users:3"},
		expectedCount: 3,
		expectedError: false,
		validateValue: func(t *testing.T, result map[string][]byte) {
			assert.Equal(t, 3, len(result))
			var user1 map[string]any
			json.Unmarshal(result["users:1"], &user1)
			assert.Equal(t, "Alice", user1["name"])
		},
	},
	{
		name:          "BulkGet with some nonexisting keys",
		keys:          []string{"users:1", "users:999", "products:1"},
		expectedCount: 2,
		expectedError: false,
		validateValue: func(t *testing.T, result map[string][]byte) {
			assert.Equal(t, 2, len(result))
			_, exists := result["users:999"]
			assert.False(t, exists)
		},
	},
	{
		name:          "BulkGet all nonexisting keys",
		keys:          []string{"nonexistent:1", "nonexistent:2"},
		expectedCount: 0,
		expectedError: false,
		validateValue: nil,
	},
	{
		name:          "BulkGet empty keys array",
		keys:          []string{},
		expectedCount: 0,
		expectedError: false,
		validateValue: nil,
	},
	{
		name:          "BulkGet mixed types",
		keys:          []string{"users:1", "user:1:ordersCount"},
		expectedCount: 2,
		expectedError: false,
		validateValue: func(t *testing.T, result map[string][]byte) {
			assert.Equal(t, 2, len(result))
			count := utils.BytesToUint64(result["user:1:ordersCount"])
			assert.Equal(t, uint64(2), count)
		},
	},
}

func TestBulkGet(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []bulkGetTC{}
	for _, tc := range bulkGetTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range bulkGetTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.db.BulkGet(ctx, tc.keys)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedCount, len(result))

				if tc.validateValue != nil {
					tc.validateValue(t, result)
				}
			}
		})
	}
}

type countTC struct {
	name     string
	prefix   string
	expected int
	db       *Badger
}

var countTestcases = []countTC{
	{
		name:     "Count with base key prefix",
		prefix:   "users:",
		expected: 3,
	},
	{
		name:     "Count with second level key prefix",
		prefix:   "user:1:orders:",
		expected: 2,
	},
	{
		name:     "Count nonexisting prefix",
		prefix:   "nonexistent:",
		expected: 0,
	},
	{
		name:     "Count with exact key match",
		prefix:   "users:1",
		expected: 1,
	},
}

func TestCount(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []countTC{}
	for _, tc := range countTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range countTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.db.Count(ctx, tc.prefix)
			assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
		})
	}
}

type decrByTC struct {
	name          string
	key           string
	delta         uint64
	expected      uint64
	expectedError bool
	db            *Badger
}

var decrByTestcases = []decrByTC{
	{
		name:          "Decrement existing counter",
		key:           "user:1:ordersCount",
		delta:         1,
		expected:      1,
		expectedError: false,
	},
	{
		name:          "Decrement by more than current value",
		key:           "user:2:ordersCount",
		delta:         5,
		expected:      0,
		expectedError: false,
	},
	{
		name:          "Decrement nonexisting key",
		key:           "user:3:ordersCount",
		delta:         3,
		expected:      0,
		expectedError: false,
	},
}

func TestDecrBy(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []decrByTC{}
	for _, tc := range decrByTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range decrByTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.DecrBy(ctx, tc.key, tc.delta)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				b, found, err := tc.db.Get(ctx, tc.key)
				assert.NoError(t, err)
				assert.True(t, found, "Key %s not found after DecrBy", tc.key)

				actual := utils.BytesToUint64(b)
				assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
			}
		})
	}
}

func TestDecrByWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []decrByTC{}
	for _, tc := range decrByTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range decrByTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return tc.db.DecrByWithTxn(ctx, txn, tc.key, tc.delta)
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				b, found, err := tc.db.Get(ctx, tc.key)
				assert.NoError(t, err)
				assert.True(t, found, "Key %s not found after DecrBy", tc.key)

				actual := utils.BytesToUint64(b)
				assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
			}
		})
	}
}

type deleteTC struct {
	name          string
	key           string
	expectedError bool
	db            *Badger
}

var deleteTestcases = []deleteTC{
	{
		name:          "Delete existing key",
		key:           "users:3",
		expectedError: false,
	},
	{
		name:          "Delete nonexisting key",
		key:           "nonexistent:key",
		expectedError: false,
	},
	{
		name:          "Delete nested key",
		key:           "user:1:orders:1",
		expectedError: false,
	},
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []deleteTC{}
	for _, tc := range deleteTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range deleteTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			// Verify key exists before delete (if it should exist)
			existsBefore := tc.db.Exists(ctx, tc.key)

			err := tc.db.Delete(ctx, tc.key)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Verify key doesn't exist after delete
				existsAfter := tc.db.Exists(ctx, tc.key)
				assert.False(t, existsAfter, "Key should not exist after delete")

				if existsBefore {
					assert.True(t, existsBefore, "Key existed before delete")
				}
			}
		})
	}
}

func TestDeleteWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []deleteTC{}
	for _, tc := range deleteTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range deleteTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			existsBefore := tc.db.Exists(ctx, tc.key)

			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return tc.db.DeleteWithTxn(ctx, txn, tc.key)
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				existsAfter := tc.db.Exists(ctx, tc.key)
				assert.False(t, existsAfter, "Key should not exist after delete")

				if existsBefore {
					assert.True(t, existsBefore, "Key existed before delete")
				}
			}
		})
	}
}

type deleteByPrefixTC struct {
	name           string
	prefix         string
	expectedError  bool
	verifyDeletion func(context.Context, *testing.T, *Badger, string)
	db             *Badger
}

var deleteByPrefixTestcases = []deleteByPrefixTC{
	{
		name:          "Delete nested prefix",
		prefix:        "user:1:orders:",
		expectedError: false,
		verifyDeletion: func(ctx context.Context, t *testing.T, store *Badger, prefix string) {
			count := store.Count(ctx, prefix)
			assert.Equal(t, 0, count, "All keys with prefix should be deleted")

			count = store.Count(ctx, "user:1")
			assert.Equal(t, 1, count, "Other keys should remain unaffected")
		},
	},
	{
		name:          "Delete by nonexisting prefix",
		prefix:        "nonexistent:prefix:",
		expectedError: false,
		verifyDeletion: func(ctx context.Context, t *testing.T, store *Badger, prefix string) {
			count := store.Count(ctx, prefix)
			assert.Equal(t, 0, count)
		},
	},
	{
		name:          "Delete all users",
		prefix:        "users:",
		expectedError: false,
		verifyDeletion: func(ctx context.Context, t *testing.T, store *Badger, prefix string) {
			count := store.Count(ctx, prefix)
			assert.Equal(t, 0, count, "All users should be deleted")
		},
	},
}

func TestDeleteByPrefix(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []deleteByPrefixTC{}
	for _, tc := range deleteByPrefixTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range deleteByPrefixTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.DeleteByPrefix(ctx, tc.prefix)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifyDeletion != nil {
					tc.verifyDeletion(ctx, t, tc.db, tc.prefix)
				}
			}
		})
	}
}

func TestDeleteByPrefixWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []deleteByPrefixTC{}
	for _, tc := range deleteByPrefixTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range deleteByPrefixTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				tc.db.DeleteByPrefixWithTxn(ctx, txn, tc.prefix)
				return nil
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifyDeletion != nil {
					tc.verifyDeletion(ctx, t, tc.db, tc.prefix)
				}
			}
		})
	}
}

type existTC struct {
	name     string
	key      string
	expected bool
	db       *Badger
}

var existTestcases = []existTC{
	{
		name:     "Existing Key",
		key:      "users:1",
		expected: true,
	},
	{
		name:     "Non existing Key",
		key:      "users:10",
		expected: false,
	},
	{
		name:     "Multilevel existing Key",
		key:      "user:1:orders:1",
		expected: true,
	},
}

func TestExists(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []existTC{}
	for _, tc := range existTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range existTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.db.Exists(ctx, tc.key)
			assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
		})
	}
}

func TestGetBadger(t *testing.T) {
	mockKVStore, teardown := setupMockDB(false)
	defer teardown()

	t.Run("Get BadgerDB instance", func(t *testing.T) {
		instance := mockKVStore.GetBadger()
		assert.NotNil(t, instance)
		assert.Equal(t, mockKVStore.db, instance)
	})
}

type getTC struct {
	name          string
	key           string
	expectedExist bool
	expectedError bool
	db            *Badger
	validateValue func(*testing.T, []byte)
}

var getTestcases = []getTC{
	{
		name:          "Get existing user",
		key:           "products:1",
		expectedExist: true,
		expectedError: false,
		validateValue: func(t *testing.T, value []byte) {
			var user map[string]any
			err := json.Unmarshal(value, &user)
			assert.NoError(t, err)
			assert.Equal(t, float64(1), user["id"])          // int
			assert.Equal(t, "Mobile", user["name"])          // string
			assert.Equal(t, 21999.99, user["price"])         // float
			assert.Equal(t, true, user["inStock"])           // bool
			assert.Equal(t, []any{"trending"}, user["tags"]) // array

			timestamp := user["addedAt"].(map[string]any)
			timestampSecs := int64(timestamp["ms"].(float64))
			assert.Equal(t, time.Unix(1767976163, 0), time.Unix(timestampSecs, 0)) // nested object
		},
	},
	{
		name:          "Get existing counter",
		key:           "user:1:ordersCount",
		expectedExist: true,
		expectedError: false,
		validateValue: func(t *testing.T, value []byte) {
			count := utils.BytesToUint64(value)
			assert.Equal(t, uint64(2), count)
		},
	},
	{
		name:          "Get nonexisting key",
		key:           "nonexistent:key",
		expectedExist: false,
		expectedError: false,
		validateValue: nil,
	},
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []getTC{}
	for _, tc := range getTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range getTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			value, exist, err := tc.db.Get(ctx, tc.key)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedExist, exist, "Expected exist: %v, Got: %v", tc.expectedExist, exist)

				if tc.expectedExist && tc.validateValue != nil {
					tc.validateValue(t, value)
				}
			}
		})
	}
}

func TestGetWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []getTC{}
	for _, tc := range getTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range getTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			var value []byte
			var exist bool

			err := tc.db.newReadonlyTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				var err error
				value, exist, err = tc.db.GetWithTxn(ctx, txn, tc.key)
				return err
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedExist, exist, "Expected exist: %v, Got: %v", tc.expectedExist, exist)

				if tc.expectedExist && tc.validateValue != nil {
					tc.validateValue(t, value)
				}
			}
		})
	}
}

type getByPrefix struct {
	name          string
	prefix        string
	expectedCount int
	expectedError bool
	db            *Badger
	validateValue func(*testing.T, map[string][]byte)
}

var getByPrefixTestcases = []getByPrefix{
	{
		name:          "Get all users",
		prefix:        "users:",
		expectedCount: 3,
		expectedError: false,
		validateValue: func(t *testing.T, result map[string][]byte) {
			assert.Equal(t, 3, len(result))
			var user1 map[string]any
			json.Unmarshal(result["users:1"], &user1)
			assert.Equal(t, "Alice", user1["name"])
		},
	},
	{
		name:          "Get all products",
		prefix:        "products:",
		expectedCount: 3,
		expectedError: false,
		validateValue: func(t *testing.T, result map[string][]byte) {
			assert.Equal(t, 3, len(result))
		},
	},
	{
		name:          "Get user orders",
		prefix:        "user:1:orders:",
		expectedCount: 2,
		expectedError: false,
		validateValue: func(t *testing.T, result map[string][]byte) {
			assert.Equal(t, 2, len(result))
			var order map[string]any
			json.Unmarshal(result["user:1:orders:1"], &order)
			assert.Equal(t, float64(1), order["id"])
		},
	},
	{
		name:          "Get with nonexisting prefix",
		prefix:        "nonexistent:",
		expectedCount: 0,
		expectedError: false,
		validateValue: nil,
	},
}

func TestGetByPrefix(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []getByPrefix{}
	for _, tc := range getByPrefixTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range getByPrefixTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.db.GetByPrefix(ctx, tc.prefix)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedCount, len(result))

				if tc.validateValue != nil {
					tc.validateValue(t, result)
				}
			}
		})
	}
}

type hasChildTC struct {
	name     string
	prefix   string
	expected bool
	db       *Badger
}

var hasChildTestcases = []hasChildTC{
	{
		name:     "Has children users",
		prefix:   "users:",
		expected: true,
	},
	{
		name:     "Has children user orders",
		prefix:   "user:1:orders:",
		expected: true,
	},
	{
		name:     "No children nonexisting prefix",
		prefix:   "user:1:payments:",
		expected: false,
	},
}

func TestHasChild(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []hasChildTC{}
	for _, tc := range hasChildTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range hasChildTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.db.HasChild(ctx, tc.prefix)
			assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
		})
	}
}

type incrByTC struct {
	name          string
	key           string
	delta         uint64
	expected      uint64
	expectedError bool
	db            *Badger
}

var incrByTestcases = []incrByTC{
	{
		name:          "Increment existing counter",
		key:           "user:1:ordersCount",
		delta:         3,
		expected:      5,
		expectedError: false,
	},
	{
		name:          "Increment by zero",
		key:           "user:2:ordersCount",
		delta:         0,
		expected:      1,
		expectedError: false,
	},
	{
		name:          "Increment nonexisting key",
		key:           "user:4:ordersCount",
		delta:         10,
		expected:      10,
		expectedError: false,
	},
	{
		name:          "Increment by large value",
		key:           "user:1:ordersCount",
		delta:         1000,
		expected:      1005, // 5 from previous test + 1000
		expectedError: false,
	},
}

func TestIncrBy(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []incrByTC{}
	for _, tc := range incrByTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range incrByTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.IncrBy(ctx, tc.key, tc.delta)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				b, found, err := tc.db.Get(ctx, tc.key)
				assert.NoError(t, err)
				assert.True(t, found, "Key %s not found after IncrBy", tc.key)

				actual := utils.BytesToUint64(b)
				assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
			}
		})
	}
}

func TestIncrByWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []incrByTC{}
	for _, tc := range incrByTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range incrByTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return tc.db.IncrByWithTxn(ctx, txn, tc.key, tc.delta)
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				b, found, err := tc.db.Get(ctx, tc.key)
				assert.NoError(t, err)
				assert.True(t, found, "Key %s not found after IncrByWithTxn", tc.key)

				actual := utils.BytesToUint64(b)
				assert.Equal(t, tc.expected, actual, "Expected: %v, Got: %v", tc.expected, actual)
			}
		})
	}
}

func TestIterateByPrefix(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()
	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()

	var testSuite = []struct {
		prefix string
		db     *Badger
	}{
		{
			prefix: "",
			db:     mockDb,
		},
		{
			prefix: "ManagedDb ",
			db:     managedMockDb,
		},
	}

	for _, ts := range testSuite {
		t.Run(ts.prefix+"Iterate all users without limit", func(t *testing.T) {
			processed := 0
			cursor, err := ts.db.IterateByPrefix(ctx, "users:", "", nil, func(k, v []byte) bool {
				processed++
				return true
			})

			assert.NoError(t, err)
			assert.Equal(t, "", cursor, "Should have no next cursor")
			assert.Equal(t, 3, processed, "Should process all 3 users")
		})

		t.Run(ts.prefix+"Iterate with limit", func(t *testing.T) {
			limit := 2
			processed := 0
			cursor, err := ts.db.IterateByPrefix(ctx, "users:", "", &limit, func(k, v []byte) bool {
				processed++
				return true
			})

			assert.NoError(t, err)
			assert.NotEmpty(t, cursor, "Should have next cursor")
			assert.Equal(t, 2, processed, "Should process only 2 users")
		})

		t.Run(ts.prefix+"Iterate with cursor continuation", func(t *testing.T) {
			limit := 1
			processed := 0
			var keys []string

			// First batch
			cursor, err := ts.db.IterateByPrefix(ctx, "users:", "", &limit, func(k, v []byte) bool {
				keys = append(keys, string(k))
				processed++
				return true
			})

			assert.NoError(t, err)
			assert.NotEmpty(t, cursor)
			assert.Equal(t, 1, processed)

			// Second batch
			cursor, err = ts.db.IterateByPrefix(ctx, "users:", cursor, &limit, func(k, v []byte) bool {
				assert.NotContains(t, keys, string(k), "Key should not have been processed before")

				keys = append(keys, string(k))
				processed++
				return true
			})

			assert.NoError(t, err)
			assert.Equal(t, 2, processed)

			// Third batch
			cursor, err = ts.db.IterateByPrefix(ctx, "users:", cursor, &limit, func(k, v []byte) bool {
				assert.NotContains(t, keys, string(k), "Key should not have been processed before")

				keys = append(keys, string(k))
				processed++
				return true
			})

			assert.NoError(t, err)
			assert.Equal(t, "", cursor, "Should have no more items")
			assert.Equal(t, 3, processed)
			assert.Equal(t, 3, len(keys))
		})

		t.Run(ts.prefix+"Iterate nonexisting prefix", func(t *testing.T) {
			processed := 0
			cursor, err := ts.db.IterateByPrefix(ctx, "users:1:payments:", "", nil, func(k, v []byte) bool {
				processed++
				return true
			})

			assert.NoError(t, err)
			assert.Equal(t, "", cursor)
			assert.Equal(t, 0, processed)
		})

		t.Run(ts.prefix+"Iterate by applying filters", func(t *testing.T) {
			count := ts.db.Count(ctx, "products:")
			assert.Equal(t, 3, count, "Precondition: should have 3 products")

			processed := 0
			cursor, err := ts.db.IterateByPrefix(ctx, "products:", "", nil, func(k, v []byte) bool {
				var product map[string]any
				json.Unmarshal(v, &product)

				// Filter: only process products that are in stock
				if !product["inStock"].(bool) {
					return false
				}

				processed++
				return true
			})

			assert.NoError(t, err)
			assert.Equal(t, "", cursor)
			assert.Equal(t, 2, processed, "Should process only 2 products which are in stock")
		})
	}
}

func TestNewTransaction(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()
	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()

	var testSuite = []struct {
		prefix string
		db     *Badger
	}{
		{
			prefix: "",
			db:     mockDb,
		},
		{
			prefix: "ManagedDb ",
			db:     managedMockDb,
		},
	}

	for _, ts := range testSuite {
		t.Run(ts.prefix+"Read-only transaction", func(t *testing.T) {
			var value []byte
			var exist bool

			err := ts.db.newReadonlyTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				var err error
				value, exist, err = ts.db.GetWithTxn(ctx, txn, "users:1")
				return err
			})

			assert.NoError(t, err)
			assert.True(t, exist)
			assert.NotNil(t, value)
		})

		t.Run(ts.prefix+"Write transaction", func(t *testing.T) {
			key := "test:txn:write"
			setValue := []byte("transaction-test")

			err := ts.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return ts.db.SetWithTxn(ctx, txn, key, setValue)
			})

			assert.NoError(t, err)

			// Verify write
			value, exist, err := ts.db.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)
			assert.Equal(t, setValue, value)
		})

		t.Run(ts.prefix+"Transaction with error rollback", func(t *testing.T) {
			key := "test:txn:rollback"
			setValue := []byte("should-not-persist")

			err := ts.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				if err := ts.db.SetWithTxn(ctx, txn, key, setValue); err != nil {
					return err
				}
				// Simulate error
				return assert.AnError
			})

			assert.Error(t, err)

			// Verify value was not persisted
			_, exist, _ := ts.db.Get(ctx, key)
			assert.False(t, exist, "Value should not exist after transaction error")
		})

		t.Run(ts.prefix+"Multiple operations in transaction", func(t *testing.T) {
			keys := []string{"test:multi:txn:1", "test:multi:txn:2", "test:multi:txn:3"}
			values := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}

			err := ts.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				for i, key := range keys {
					if err := ts.db.SetWithTxn(ctx, txn, key, values[i]); err != nil {
						return err
					}
				}
				return nil
			})

			assert.NoError(t, err)

			// Verify all writes
			for i, key := range keys {
				value, exist, err := ts.db.Get(ctx, key)
				assert.NoError(t, err)
				assert.True(t, exist)
				assert.Equal(t, values[i], value)
			}
		})
	}
}

type setTC struct {
	name          string
	key           string
	value         []byte
	expectedError bool
	db            *Badger
	verifySet     func(context.Context, *testing.T, *Badger, string)
}

var setTestcases = []setTC{
	{
		name:          "Set new user",
		key:           "users:4",
		value:         []byte(`{"id":4,"name":"Dave"}`),
		expectedError: false,
		verifySet: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)
			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, float64(4), user["id"])
			assert.Equal(t, "Dave", user["name"])
		},
	},
	{
		name:          "Update existing user",
		key:           "users:1",
		value:         []byte(`{"id":1,"name":"Alice Updated"}`),
		expectedError: false,
		verifySet: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)
			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, "Alice Updated", user["name"])
		},
	},
	{
		name:          "Set new counter",
		key:           "users:4:ordersCount",
		value:         utils.Uint64ToBytes(5),
		expectedError: false,
		verifySet: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)
			count := utils.BytesToUint64(value)
			assert.Equal(t, uint64(5), count)
		},
	},
	{
		name:          "Set empty value",
		key:           "test:empty",
		value:         []byte{},
		expectedError: false,
		verifySet: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)
			assert.Equal(t, 0, len(value))
		},
	},
	{
		name:          "Set with nested key",
		key:           "users:4:profile:settings:theme",
		value:         []byte("dark"),
		expectedError: false,
		verifySet: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)
			assert.Equal(t, "dark", string(value))
		},
	},
}

func TestSet(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []setTC{}
	for _, tc := range setTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range setTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.Set(ctx, tc.key, tc.value)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifySet != nil {
					tc.verifySet(ctx, t, tc.db, tc.key)
				}
			}
		})
	}
}

func TestSetWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []setTC{}
	for _, tc := range setTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range setTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return tc.db.SetWithTxn(ctx, txn, tc.key, tc.value)
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifySet != nil {
					tc.verifySet(ctx, t, tc.db, tc.key)
				}
			}
		})
	}
}

type updateJsonTC struct {
	name          string
	key           string
	initialValue  []byte
	delta         map[string]any
	expectedError bool
	db            *Badger
	verifyUpdate  func(context.Context, *testing.T, *Badger, string)
}

var updateJsonTestcases = []updateJsonTC{
	{
		name:          "Update existing JSON object - add new field",
		key:           "users:1",
		initialValue:  []byte(`{"id":1,"name":"Alice"}`),
		delta:         map[string]any{"age": 30},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, float64(1), user["id"])
			assert.Equal(t, "Alice", user["name"])
			assert.Equal(t, float64(30), user["age"])
		},
	},
	{
		name:          "Update existing JSON object - modify field",
		key:           "users:2",
		initialValue:  []byte(`{"id":2,"name":"Bob","age":25}`),
		delta:         map[string]any{"name": "Robert", "age": 26},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, float64(2), user["id"])
			assert.Equal(t, "Robert", user["name"])
			assert.Equal(t, float64(26), user["age"])
		},
	},
	{
		name:          "Update non-existing key",
		key:           "users:999",
		initialValue:  nil,
		delta:         map[string]any{"id": 999, "name": "NewUser"},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, float64(999), user["id"])
			assert.Equal(t, "NewUser", user["name"])
		},
	},
	{
		name:          "Update with empty delta",
		key:           "users:3",
		initialValue:  []byte(`{"id":3,"name":"Charlie"}`),
		delta:         map[string]any{},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, float64(3), user["id"])
			assert.Equal(t, "Charlie", user["name"])
		},
	},
	{
		name:          "Update with nested JSON",
		key:           "users:4",
		initialValue:  []byte(`{"id":4,"name":"Dave"}`),
		delta:         map[string]any{"address": map[string]any{"city": "NYC", "zip": "10001"}},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			var user map[string]any
			json.Unmarshal(value, &user)
			assert.Equal(t, float64(4), user["id"])
			assert.Equal(t, "Dave", user["name"])
			address := user["address"].(map[string]any)
			assert.Equal(t, "NYC", address["city"])
			assert.Equal(t, "10001", address["zip"])
		},
	},
}

func TestUpdateJson(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []updateJsonTC{}
	for _, tc := range updateJsonTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range updateJsonTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			// Setup initial value if provided
			if tc.initialValue != nil {
				err := tc.db.Set(ctx, tc.key, tc.initialValue)
				assert.NoError(t, err)
			}

			err := tc.db.UpdateJson(ctx, tc.key, tc.delta)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifyUpdate != nil {
					tc.verifyUpdate(ctx, t, tc.db, tc.key)
				}
			}
		})
	}
}

func TestUpdateJsonWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []updateJsonTC{}
	for _, tc := range updateJsonTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range updateJsonTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			// Setup initial value if provided
			if tc.initialValue != nil {
				err := tc.db.Set(ctx, tc.key, tc.initialValue)
				assert.NoError(t, err)
			}

			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return tc.db.UpdateJsonWithTxn(ctx, txn, tc.key, tc.delta)
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifyUpdate != nil {
					tc.verifyUpdate(ctx, t, tc.db, tc.key)
				}
			}
		})
	}
}

type updateProtobufTC struct {
	name          string
	key           string
	initialValue  *types.Command
	delta         *types.Command
	expectedError bool
	db            *Badger
	verifyUpdate  func(context.Context, *testing.T, *Badger, string)
}

var updateProtobufTestcases = []updateProtobufTC{
	{
		name: "Update existing protobuf - add new field",
		key:  "command:1",
		initialValue: &types.Command{
			Type:  types.CommandType_SET,
			Key:   "test:key",
			Value: []byte("test value"),
		},
		delta: &types.Command{
			Ttl: durationpb.New(5 * time.Minute),
		},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			cmd, err := types.DecodeCommand(value)
			assert.NoError(t, err)
			assert.Equal(t, types.CommandType_SET, cmd.Type)
			assert.Equal(t, "test:key", cmd.Key)
			assert.Equal(t, []byte("test value"), cmd.Value)
			assert.NotNil(t, cmd.Ttl)
			assert.Equal(t, int64(5*time.Minute), cmd.Ttl.AsDuration().Nanoseconds())
		},
	},
	{
		name: "Update existing protobuf - modify fields",
		key:  "command:2",
		initialValue: &types.Command{
			Type:            types.CommandType_INCR_BY,
			Key:             "counter:1",
			IncrOrDecrDelta: 5,
		},
		delta: &types.Command{
			IncrOrDecrDelta: 10,
			Prefix:          "counter:2",
		},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			cmd, err := types.DecodeCommand(value)
			assert.NoError(t, err)
			assert.Equal(t, types.CommandType_INCR_BY, cmd.Type)
			assert.Equal(t, "counter:1", cmd.Key)
			assert.Equal(t, uint64(10), cmd.IncrOrDecrDelta)
			assert.Equal(t, "counter:2", cmd.Prefix)
		},
	},
	{
		name:         "Update non-existing protobuf key",
		key:          "command:999",
		initialValue: nil,
		delta: &types.Command{
			Type: types.CommandType_DELETE,
			Key:  "test:key",
		},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			cmd, err := types.DecodeCommand(value)
			assert.NoError(t, err)
			assert.Equal(t, types.CommandType_DELETE, cmd.Type)
			assert.Equal(t, "test:key", cmd.Key)
		},
	},
	{
		name: "Update with empty delta",
		key:  "command:3",
		initialValue: &types.Command{
			Type: types.CommandType_TRANSACTION,
			SubCommands: []*types.Command{
				{Type: types.CommandType_SET, Key: "key1", Value: []byte("val1")},
			},
		},
		delta:         &types.Command{},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			cmd, err := types.DecodeCommand(value)
			assert.NoError(t, err)
			assert.Equal(t, types.CommandType_TRANSACTION, cmd.Type)
			assert.Equal(t, 1, len(cmd.SubCommands))
			assert.Equal(t, types.CommandType_SET, cmd.SubCommands[0].Type)
		},
	},
	{
		name: "Update with nested protobuf fields",
		key:  "command:4",
		initialValue: &types.Command{
			Type: types.CommandType_TRANSACTION,
			SubCommands: []*types.Command{
				{Type: types.CommandType_SET, Key: "key1", Value: []byte("val1")},
			},
		},
		delta: &types.Command{
			SubCommands: []*types.Command{
				{Type: types.CommandType_SET, Key: "key1", Value: []byte("val1")},
				{Type: types.CommandType_DELETE, Key: "key2"},
			},
		},
		expectedError: false,
		verifyUpdate: func(ctx context.Context, t *testing.T, store *Badger, key string) {
			value, exist, err := store.Get(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exist)

			cmd, err := types.DecodeCommand(value)
			assert.NoError(t, err)
			assert.Equal(t, types.CommandType_TRANSACTION, cmd.Type)
			assert.Equal(t, 2, len(cmd.SubCommands))
			assert.Equal(t, types.CommandType_SET, cmd.SubCommands[0].Type)
			assert.Equal(t, types.CommandType_DELETE, cmd.SubCommands[1].Type)
		},
	},
}

func TestUpdateProtobuf(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []updateProtobufTC{}
	for _, tc := range updateProtobufTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range updateProtobufTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			// Setup initial value if provided
			if tc.initialValue != nil {
				initialData, err := tc.initialValue.Encode()
				assert.NoError(t, err)
				err = tc.db.Set(ctx, tc.key, initialData)
				assert.NoError(t, err)
			}

			err := tc.db.UpdateProtobuf(ctx, tc.key, tc.delta)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifyUpdate != nil {
					tc.verifyUpdate(ctx, t, tc.db, tc.key)
				}
			}
		})
	}
}

func TestUpdateProtobufWithTxn(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()

	tcList := []updateProtobufTC{}
	for _, tc := range updateProtobufTestcases {
		tc.db = mockDb
		tcList = append(tcList, tc)
	}

	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()
	for _, tc := range updateProtobufTestcases {
		tc.name = "ManagedDb " + tc.name
		tc.db = managedMockDb
		tcList = append(tcList, tc)
	}

	for _, tc := range tcList {
		t.Run(tc.name, func(t *testing.T) {
			// Setup initial value if provided
			if tc.initialValue != nil {
				initialData, err := tc.initialValue.Encode()
				assert.NoError(t, err)
				err = tc.db.Set(ctx, tc.key, initialData)
				assert.NoError(t, err)
			}

			err := tc.db.NewTransaction(ctx, func(ctx context.Context, txn *badger.Txn) error {
				return tc.db.UpdateProtobufWithTxn(ctx, txn, tc.key, tc.delta)
			})

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.verifyUpdate != nil {
					tc.verifyUpdate(ctx, t, tc.db, tc.key)
				}
			}
		})
	}
}

func TestWriteBatch(t *testing.T) {
	ctx := context.Background()
	mockDb, teardown := setupMockDB(false)
	defer teardown()
	managedMockDb, managedDbteardown := setupMockDB(true)
	defer managedDbteardown()

	var testSuite = []struct {
		prefix string
		db     *Badger
	}{
		{
			prefix: "",
			db:     mockDb,
		},
		{
			prefix: "ManagedDb ",
			db:     managedMockDb,
		},
	}

	for _, ts := range testSuite {
		t.Run(ts.prefix+"Write batch with set operations", func(t *testing.T) {
			ops := []*types.Command{
				{Type: types.CommandType_SET, Key: "batch:1", Value: []byte("value1")},
				{Type: types.CommandType_SET, Key: "batch:2", Value: []byte("value2")},
				{Type: types.CommandType_SET, Key: "batch:3", Value: []byte("value3")},
			}

			err := ts.db.batchWrite(ctx, ops)
			assert.NoError(t, err)

			// Verify all values
			for i, op := range ops {
				value, exist, err := ts.db.Get(ctx, op.Key)
				assert.NoError(t, err)
				assert.True(t, exist)
				assert.Equal(t, []byte("value"+string(rune('1'+i))), value)
			}
		})

		t.Run(ts.prefix+"Write batch with delete operations", func(t *testing.T) {
			// setup
			ts.db.Set(ctx, "batch:del:1", []byte("value1"))
			ts.db.Set(ctx, "batch:del:2", []byte("value2"))

			assert.True(t, ts.db.Exists(ctx, "batch:del:1"))
			assert.True(t, ts.db.Exists(ctx, "batch:del:2"))

			ops := []*types.Command{
				{Type: types.CommandType_DELETE, Key: "batch:del:1"},
				{Type: types.CommandType_DELETE, Key: "batch:del:2"},
			}

			err := ts.db.batchWrite(ctx, ops)
			assert.NoError(t, err)

			for _, op := range ops {
				assert.False(t, ts.db.Exists(ctx, op.Key))
			}
		})

		t.Run(ts.prefix+"Write batch with mixed operations", func(t *testing.T) {
			ts.db.Set(ctx, "batch:mixed:2", []byte("{\"id\": 1010, \"name\": \"Old Name\"}"))
			initialProtoData, _ := (&types.Command{Type: types.CommandType_SET}).Encode()
			ts.db.Set(ctx, "batch:mixed:3", initialProtoData)
			ts.db.Set(ctx, "batch:mixed:del", []byte("to-delete"))
			ts.db.Set(ctx, "batch:mixed:counter1", utils.Uint64ToBytes(10))
			ts.db.Set(ctx, "batch:mixed:counter2", utils.Uint64ToBytes(10))

			newProto := &types.Command{Key: "something"}
			newProtoData, _ := newProto.Encode()
			msgName, md := types.GetFileDescriptorSet(newProto)
			ops := []*types.Command{
				{Type: types.CommandType_SET, Key: "batch:mixed:1", Value: []byte("set1"), Ttl: durationpb.New(1 * time.Minute)},
				{Type: types.CommandType_UPDATE, Key: "batch:mixed:2", Value: []byte("{\"name\": \"new name\"}"), Ttl: durationpb.New(1 * time.Minute),
					UpdateMeta: &types.UpdateMeta{UpdateDeltaType: types.UpdateDeltaType_JSON}},
				{Type: types.CommandType_UPDATE, Key: "batch:mixed:3", Value: newProtoData, Ttl: durationpb.New(1 * time.Minute),
					UpdateMeta: &types.UpdateMeta{UpdateDeltaType: types.UpdateDeltaType_PROTOBUF, MessageDescriptors: md, MessageName: msgName}},
				{Type: types.CommandType_DELETE, Key: "batch:mixed:del"},
				{Type: types.CommandType_INCR_BY, Key: "batch:mixed:counter1", IncrOrDecrDelta: 5},
				{Type: types.CommandType_DECR_BY, Key: "batch:mixed:counter2", IncrOrDecrDelta: 5},
			}

			err := ts.db.batchWrite(ctx, ops)
			assert.NoError(t, err)

			// Verify set operations
			value, exist, _ := ts.db.Get(ctx, "batch:mixed:1")
			assert.True(t, exist)
			assert.Equal(t, []byte("set1"), value)

			// Verify update JSON operation
			value, exist, _ = ts.db.Get(ctx, "batch:mixed:2")
			assert.True(t, exist)

			var updated map[string]any
			json.Unmarshal(value, &updated)
			assert.Equal(t, float64(1010), updated["id"])
			assert.Equal(t, "new name", updated["name"])

			// Verify update Protobuf operation
			value, exist, _ = ts.db.Get(ctx, "batch:mixed:3")
			assert.True(t, exist)

			updatedProto, err := types.DecodeCommand(value)
			assert.NoError(t, err)
			assert.Equal(t, "something", updatedProto.Key)

			// Verify delete operation
			exist = ts.db.Exists(ctx, "batch:mixed:del")
			assert.False(t, exist)

			// Verify incr operation
			value, exist, _ = ts.db.Get(ctx, "batch:mixed:counter1")
			assert.True(t, exist)
			assert.Equal(t, uint64(15), utils.BytesToUint64(value))

			// Verify decr operation
			value, exist, _ = ts.db.Get(ctx, "batch:mixed:counter2")
			assert.True(t, exist)
			assert.Equal(t, uint64(5), utils.BytesToUint64(value))
		})

		t.Run(ts.prefix+"Write batch should not write any operation on 1 fail", func(t *testing.T) {
			ts.db.Set(ctx, "batch:mixederr:del", []byte("to-delete"))
			ts.db.Set(ctx, "batch:mixederr:counter1", utils.Uint64ToBytes(10))
			ts.db.Set(ctx, "batch:mixederr:counter2", utils.Uint64ToBytes(10))

			ops := []*types.Command{
				{Type: types.CommandType_SET, Key: "batch:mixederr:1", Value: []byte("set1"), Ttl: durationpb.New(1 * time.Minute)},
				{Type: types.CommandType_DELETE, Key: "batch:mixederr:del"},
				{Type: types.CommandType_INCR_BY, Key: "batch:mixederr:counter1", IncrOrDecrDelta: 5},
				{Type: types.CommandType_UNKNOWN, Key: "batch:mixederr:invalid", Value: []byte("set1")},
				{Type: types.CommandType_DECR_BY, Key: "batch:mixederr:counter2", IncrOrDecrDelta: 5},
			}

			err := ts.db.batchWrite(ctx, ops)
			assert.Error(t, err)

			// Verify set operations
			value, exist, _ := ts.db.Get(ctx, "batch:mixederr:1")
			assert.False(t, exist)

			// Verify delete operation
			exist = ts.db.Exists(ctx, "batch:mixederr:del")
			assert.True(t, exist)

			// Verify incr operation
			value, exist, _ = ts.db.Get(ctx, "batch:mixederr:counter1")
			assert.True(t, exist)
			assert.Equal(t, uint64(10), utils.BytesToUint64(value))

			// Verify decr operation
			value, exist, _ = ts.db.Get(ctx, "batch:mixederr:counter2")
			assert.True(t, exist)
			assert.Equal(t, uint64(10), utils.BytesToUint64(value))
		})

		t.Run(ts.prefix+"Write batch with more then allowed ops", func(t *testing.T) {
			ops := make([]*types.Command, MaxBatchSize+1)
			for i := range MaxBatchSize + 1 {
				ops[i] = &types.Command{Type: types.CommandType_SET, Key: "batch:overflow:" + string(rune('A'+i)), Value: []byte("value")}
			}

			err := ts.db.batchWrite(ctx, ops)
			assert.Equal(t, err, ErrBatchTooBig)
		})

		t.Run(ts.prefix+"Write batch with empty operations", func(t *testing.T) {
			ops := []*types.Command{}

			err := ts.db.batchWrite(ctx, ops)
			assert.NoError(t, err)
		})

		t.Run(ts.prefix+"Write batch with nil operations", func(t *testing.T) {
			err := ts.db.batchWrite(ctx, nil)
			assert.NoError(t, err)
		})

		t.Run(ts.prefix+"Write batch with invalid operation", func(t *testing.T) {
			ops := []*types.Command{
				{Type: types.CommandType_UNKNOWN, Key: "invalid:op"},
			}

			err := ts.db.batchWrite(ctx, ops)
			assert.Equal(t, err, ErrUnknownOp)
		})
	}
}

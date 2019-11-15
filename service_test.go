package datastore

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("DATASTORE_EMULATOR_HOST", "localhost:8081")
	_ = os.Setenv("PATH", "")
	code := m.Run()
	os.Exit(code)
}

func TestDatastoreManager_Create(t *testing.T) {
	type Test struct {
		Name        string
		Description string
	}
	setValue := Test{"Anthony Gonzalves", "Duniya mei Akala hu"}
	var getValue Test

	ctx, done, err := aetest.NewContext()
	assert.Nil(t, err)
	defer done()

	man := NewDatastoreManager("testPackage")

	key, _ := man.Create(ctx, "test_1", 0, &setValue)
	assert.NotNil(t, key)

	err = man.Get(ctx, key.(*datastore.Key), &getValue)
	assert.Nil(t, err)

	err = man.Delete(ctx, key.(*datastore.Key))
	assert.Nil(t, err)
	assert.Equal(t, "Anthony Gonzalves", getValue.Name)
	assert.Equal(t, "Duniya mei Akala hu", getValue.Description)
}

func TestDatastoreManager_Create2(t *testing.T) {
	type Test struct {
		Name        string
		Description string
	}
	setValue := Test{"Anthony Gonzalves", "Duniya mei Akala hu"}
	var getValue Test

	ctx, done, err := aetest.NewContext()
	assert.Nil(t, err)
	defer done()

	man := NewDatastoreManager("testPackage")

	key, _ := man.Create(ctx, "", 100, &setValue)
	assert.NotNil(t, key)

	err = man.Get(ctx, key.(*datastore.Key), &getValue)
	assert.Nil(t, err)

	err = man.Delete(ctx, key.(*datastore.Key))
	assert.Nil(t, err)
	assert.Equal(t, "Anthony Gonzalves", getValue.Name)
	assert.Equal(t, "Duniya mei Akala hu", getValue.Description)
}

func TestDatastoreManager_Create3(t *testing.T) {
	type Test struct {
		Name        string
		Description string
	}
	setValue := Test{"Anthony Gonzalves", "Duniya mei Akala hu"}
	var getValue Test

	ctx, done, err := aetest.NewContext()
	assert.Nil(t, err)
	defer done()

	man := NewDatastoreManager("testPackage")

	key, _ := man.Create(ctx, "", 0, &setValue)
	assert.NotNil(t, key)

	err = man.Get(ctx, key.(*datastore.Key), &getValue)
	assert.Nil(t, err)

	err = man.Delete(ctx, key.(*datastore.Key))
	assert.Nil(t, err)
	assert.Equal(t, "Anthony Gonzalves", getValue.Name)
	assert.Equal(t, "Duniya mei Akala hu", getValue.Description)
}

func TestDatastoreManager_Create4(t *testing.T) {
	type Test struct {
		Name        string
		Description string
	}
	setValue := Test{"Anthony Gonzalves", "Duniya mei Akala hu"}
	var getValue Test

	ctx, done, err := aetest.NewContext()
	assert.Nil(t, err)
	defer done()

	man := NewDatastoreManager("testPackage")

	key, _ := man.Create(ctx, "", -100, &setValue)
	assert.NotNil(t, key)

	err = man.Get(ctx, key.(*datastore.Key), &getValue)
	assert.Nil(t, err)

	err = man.Delete(ctx, key.(*datastore.Key))
	assert.Nil(t, err)
	assert.Equal(t, "Anthony Gonzalves", getValue.Name)
	assert.Equal(t, "Duniya mei Akala hu", getValue.Description)
}

func TestDatastoreManager_Query(t *testing.T) {
	type Test struct {
		Id        string
		RefId     string
		Raw       string
		Timestamp time.Time
		Foreign   string
		Status    string
	}

	numbers := []string{"67890", "12345"}

	ctx, done, err := aetest.NewContext()
	assert.Nil(t, err)
	defer done()

	uuidR1 := uuid.NewV4()
	uuidR2 := uuid.NewV1()

	uuidR := []string{uuidR1.String(), uuidR2.String()}

	nowTime := time.Now()

	man := NewDatastoreManager("TestPackage")
	for idx := 0; idx < 1200; idx++ {
		var test Test

		uuid := uuid.NewV4()
		test.Id = uuid.String()

		if idx%2 == 0 {
			test.Foreign = numbers[0]
			test.RefId = uuidR[0]
		} else {
			test.Foreign = numbers[1]
			test.RefId = uuidR[1]
		}
		test.Raw = "Life is Beautiful"
		test.Status = "Sent"
		test.Timestamp = nowTime.Add(time.Duration(rand.Int()))

		key, err := man.Create(ctx, test.Id, 0, &test)
		if err != nil {
			assert.Nil(t, err, "Shouldnt be nul ", err)
		}
		assert.NotNil(t, key)
	}

	q := QueryParams{Field: "RefId", Operator: "=", Value: uuidR1.String(), SortField: "Timestamp", StartIndex: 1, EndIndex: 20, SortOrder: "ascending"}
	//q := QueryParams{Field:"Status", Operator:"<", Value: "tt",  StartIndex: 1, EndIndex: 50}

	results := make([]Test, 50)
	pass := InterfaceSlice(results)

	w, err := man.Query(ctx, &q, pass)
	fmt.Println(len(w))
	assert.Nil(t, err, "No error should be found while doing a legit query", err)

}

func InterfaceSlice(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("InterfaceSlice() given a non-slice type")
	}

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

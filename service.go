package datastore

import (
	"google.golang.org/api/iterator"
	"google.golang.org/appengine/datastore"
)

type QueryParams struct {
	Field      string
	Operator   string
	Value      interface{}
	Limit      int
	StartIndex int
	EndIndex   int
	SortOrder  string
	SortField  string
}

func NewDatastoreManager(kind string) *DatastoreManager {
	return &DatastoreManager{kind: kind}
}

type DatastoreManager struct {
	kind string
}

func (dm DatastoreManager) GetKey(ctx context.Context, key string, id int) *datastore.Key {
	return datastore.NewKey(ctx, dm.kind, key, int64(id), nil)
}

func (dm DatastoreManager) Create(ctx context.Context, key string, id int, payload interface{}) (interface{}, error) {
	return datastore.Put(ctx, dm.GetKey(ctx, key, id), payload)
}

func (dm DatastoreManager) Get(ctx context.Context, key interface{}, payload interface{}) error {
	return datastore.Get(ctx, key.(*datastore.Key), payload)
}

func (dm DatastoreManager) Delete(ctx context.Context, key interface{}) error {
	return datastore.Delete(ctx, key.(*datastore.Key))
}

func (dm DatastoreManager) DeleteAll(ctx context.Context, keys interface{}) error {
	return datastore.DeleteMulti(ctx, keys.([]*datastore.Key))
}

func (dm DatastoreManager) Count(ctx context.Context) (int, error) {
	q := datastore.NewQuery(dm.kind)
	if count, err := q.Count(ctx); err == nil {
		return count, nil
	} else {
		return -1, err
	}
}

func (dm DatastoreManager) Query(ctx context.Context, q *QueryParams, results []interface{}) ([]interface{}, error) {
	var query *datastore.Query

	if q.SortField != "" {
		var sorting string
		if q.SortOrder == "descending" {
			sorting = "-" + q.SortField
		} else {
			sorting = q.SortField
		}
		query = datastore.NewQuery(dm.kind).Filter(q.Field+" "+q.Operator, q.Value).Order(sorting)
	} else {
		query = datastore.NewQuery(dm.kind).Filter(q.Field+" "+q.Operator, q.Value)
	}

	var index int
	if q.EndIndex > 50 {
		q.EndIndex = 50
	}

	for itt := query.Run(ctx); index < q.EndIndex; index++ {

		if _, err := itt.Next(results[index]); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}

		//if q.StartIndex > index {
		//	results = append(results, result)
		//}
	}
	return nil, nil
}

func (dm DatastoreManager) GetAll(ctx context.Context, key string, val string, payloads interface{}) (interface{}, error) {
	q := datastore.NewQuery(dm.kind).Filter(key, val)
	_, err := q.GetAll(ctx, payloads)
	return payloads, err
}

func (dm DatastoreManager) GetAllQuery(ctx context.Context, q *QueryParams, results interface{}) (interface{}, error) {
	var query *datastore.Query

	if q.SortField != "" {
		var sorting string
		if q.SortOrder == "descending" {
			sorting = "-" + q.SortField
		} else {
			sorting = q.SortField
		}
		query = datastore.NewQuery(dm.kind).Filter(q.Field+" "+q.Operator, q.Value).Order(sorting)
	} else {
		query = datastore.NewQuery(dm.kind).Filter(q.Field+" "+q.Operator, q.Value)
	}
	_, err := query.GetAll(ctx, results)
	return results, err
}

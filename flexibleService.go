package datastore

import (
	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"reflect"
)

type QueryParams struct {
	Field      string
	Operator   string
	Value      interface{}
	Offset     int
	Limit      int
	StartIndex int
	EndIndex   int
	SortOrder  string
	SortField  string
	SiteId     string
	Type       string
}

type DBManagerFlex struct {
	client *datastore.Client
	kind   string
	ctx    context.Context
}

func GetClientFlex(ctx context.Context, project string) (*datastore.Client, error) {
	return datastore.NewClient(ctx, project)
}

func NewDBManagerFlex(client *datastore.Client, kind string) (*DBManagerFlex, error) {
	ctx := context.Background()
	t, err := client.NewTransaction(ctx)
	if err != nil {
		return nil, err
	}
	if err := t.Rollback(); err != nil {
		return nil, err
	}
	return &DBManagerFlex{
		client: client,
		ctx:    ctx,
		kind:   kind,
	}, nil
}

func (dm DBManagerFlex) GetKeyFlex(key string, id int) *datastore.Key {
	if key != "" && key != "0" {
		return datastore.NameKey(dm.kind, key, nil)
	} else if id > 0 {
		return datastore.IDKey(dm.kind, int64(id), nil)
	}
	return datastore.IncompleteKey(dm.kind, nil)
}

func (dm DBManagerFlex) CreateFlex(key string, id int, payload interface{}) (interface{}, error) {
	return dm.client.Put(dm.ctx, dm.GetKeyFlex(key, id), payload)
}

func (dm DBManagerFlex) GetFlex(key interface{}, payload interface{}) error {
	return dm.client.Get(dm.ctx, key.(*datastore.Key), payload)
}

func (dm DBManagerFlex) DeleteFlex(key interface{}) error {
	return dm.client.Delete(dm.ctx, key.(*datastore.Key))
}

func (dm DBManagerFlex) DeleteAllFlex(keys interface{}) error {
	return dm.client.DeleteMulti(dm.ctx, keys.([]*datastore.Key))
}

func (dm DBManagerFlex) CountFlex() (int, error) {
	q := datastore.NewQuery(dm.kind)
	return dm.client.Count(dm.ctx, q)
}

func (dm DBManagerFlex) QueryFlex(q *QueryParams, results []interface{}) ([]interface{}, error) {
	var query *datastore.Query

	query = datastore.NewQuery(dm.kind)
	if q.SiteId != "" {
		query = query.Filter("SiteId = ", q.SiteId)
	}

	if q.Type != "" {
		query = query.Filter("Type = ", q.Type)
	}
	query = query.Filter(q.Field+" "+q.Operator, q.Value)

	if q.SortField != "" {
		var sorting string
		if q.SortOrder == "descending" {
			sorting = "-" + q.SortField
		} else {
			sorting = q.SortField
		}
		query = query.Order(sorting)
	}
	var index int
	if q.EndIndex > 50 {
		q.EndIndex = 50
	}

	for itt := dm.client.Run(dm.ctx, query); index < q.EndIndex; index++ {

		if _, err := itt.Next(results[index]); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (dm DBManagerFlex) GetAllFlex(q *QueryParams, payloads interface{}) ([]interface{}, error) {
	var query *datastore.Query
	result := make([]interface{}, 0)
	var sorting string

	query = datastore.NewQuery(dm.kind)

	if q.SiteId != "" {
		query = query.Filter("SiteId = ", q.SiteId)
	}
	if q.Type != "" {
		query = query.Filter("Type = ", q.Type)
	}

	if q.SortField != "" {
		if q.SortOrder == "descending" {
			sorting = "-" + q.SortField
		} else {
			sorting = q.SortField
		}
	}

	query = query.Filter(q.Field+" "+q.Operator, q.Value).Order(sorting)
	if q.Limit != 0 {
		query = query.Limit(q.Limit)
	} else {
		query = query.Offset(q.Offset)
	}

	itt := dm.client.Run(dm.ctx, query)
	for {
		if _, err := itt.Next(payloads); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		temp := interface{}(payloads)
		v := reflect.Indirect(reflect.ValueOf(temp))
		final := v.Interface()

		result = append(result, final)
	}
	return result, nil
}

func (dm DBManagerFlex) GetAllQueryFlex(query *datastore.Query, results interface{}) (interface{}, error) {
	_, err := dm.client.GetAll(dm.ctx, query, results)
	return results, err
}

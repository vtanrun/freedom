package general

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/8treenet/freedom/general/requests"
	"github.com/go-redis/redis"
	uuid "github.com/iris-contrib/go.uuid"
)

var _ Entity = new(entity)

type Entity interface {
	DomainEvent(eventName string, object interface{}, header ...map[string]string)
	Identity() string
	GetRuntime() Runtime
	SetProducer(string)
}

type DomainEventInfra interface {
	DomainEvent(producer, topic string, data []byte, runtime Runtime, header ...map[string]string)
}

func injectBaseEntity(run Runtime, entityObject interface{}) {
	entityObjectValue := reflect.ValueOf(entityObject)
	if entityObjectValue.Kind() == reflect.Ptr {
		entityObjectValue = entityObjectValue.Elem()
	}
	entityField := entityObjectValue.FieldByName("Entity")
	if !entityField.IsNil() {
		return
	}

	e := new(entity)
	e.runtime = run
	eValue := reflect.ValueOf(e)
	if entityField.Kind() != reflect.Interface || !eValue.Type().Implements(entityField.Type()) {
		panic("This is not a valid entity")
	}
	entityField.Set(eValue)
	return
}

type entity struct {
	runtime    Runtime
	entityName string
	identity   string
	producer   string
}

func (e *entity) DomainEvent(fun string, object interface{}, header ...map[string]string) {
	if globalApp.eventInfra == nil {
		panic("Unrealized Domain Event Infrastructure.")
	}
	json, err := json.Marshal(object)
	if err != nil {
		panic(err)
	}
	globalApp.eventInfra.DomainEvent(e.producer, fun, json, e.runtime, header...)
}

func (e *entity) Identity() string {
	if e.identity == "" {
		u, _ := uuid.NewV1()
		e.identity = strings.ToLower(strings.ReplaceAll(u.String(), "-", ""))
	}
	return e.identity
}

func (e *entity) GetRuntime() Runtime {
	return e.runtime
}

func (e *entity) SetProducer(producer string) {
	e.producer = producer
}

func newEntityCache(client redis.Cmdable, expiration time.Duration) *entityCache {
	return &entityCache{
		client:      client,
		entitys:     make(map[reflect.Type]interface{}),
		entityGroup: make(map[reflect.Type]*requests.Group),
		expiration:  expiration,
	}
}

type entityCache struct {
	client      redis.Cmdable
	entitys     map[reflect.Type]interface{}
	entityGroup map[reflect.Type]*requests.Group
	expiration  time.Duration
}

func (e *entityCache) AddEntityCache(t reflect.Type, fun interface{}) {
	e.entitys[t] = fun
	e.entityGroup[t] = &requests.Group{}
}

func (ec *entityCache) GetEntityCache(entity interface{}, repo GORMRepository) error {
	entityValue := reflect.ValueOf(entity)
	entityType := entityValue.Type()
	group, ok := ec.entityGroup[entityType]
	if !ok {
		return errors.New("No callback found for entity cache")
	}
	key, err := ec.getkey(entity, entityType)
	if err != nil {
		return err
	}
	v, err, shared := group.Do(key, func() (interface{}, error) {
		data, err := ec.getEntity(entity, entityValue, key, repo)
		return data, err
	})
	if err != nil {
		return err
	}
	if shared {
		return nil
	}

	err = json.Unmarshal(v.([]byte), entity)
	if err != nil {
		return err
	}
	return nil
}

func (ec *entityCache) getEntity(entity interface{}, entityValue reflect.Value, key string, repo GORMRepository) ([]byte, error) {
	if ec.client == nil {
		return nil, errors.New("Redis is not installed for entity cache")
	}
	fun, ok := ec.entitys[entityValue.Type()]
	if !ok {
		return nil, errors.New("No callback found for entity cache")
	}

	data, e := ec.client.Get(key).Bytes()
	if e == nil {
		return data, nil
	}
	if e != nil && e != redis.Nil {
		globalApp.IrisApp.Logger().Error("Redis call error :", e.Error())
	}

	callReturn := reflect.ValueOf(fun).Call([]reflect.Value{reflect.ValueOf(entity), reflect.ValueOf(repo)})[0]
	if !callReturn.IsNil() {
		return data, callReturn.Interface().(error)
	}

	data, e = json.Marshal(entity)
	if e != nil {
		globalApp.IrisApp.Logger().Error("Entity serialization failed :", e.Error())
		return nil, e
	}
	ec.client.Set(key, data, ec.expiration)
	return data, nil
}

func (ec *entityCache) DeleteEntityCache(entity interface{}) error {
	key, err := ec.getkey(entity, reflect.TypeOf(entity))
	if err != nil {
		return err
	}
	return ec.client.Del(key).Err()
}

func (ec *entityCache) getkey(entity interface{}, t reflect.Type) (string, error) {
	var identity string
	if impl, ok := entity.(Entity); !ok {
		return "", errors.New("The parameter passed in is not a valid entity")
	} else {
		identity = impl.Identity()
	}
	return "entity:" + t.Elem().String() + ":" + identity, nil
}

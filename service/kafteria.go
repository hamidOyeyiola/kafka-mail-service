package kafteria

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/hamidOyeyiola/kafka-mail-service/utils"
	"github.com/segmentio/kafka-go"

	"google.golang.org/grpc"
)

const (
	availableClientConns = 10
)

type kafkaConn struct {
	reader     *kafka.Reader
	writer     *kafka.Writer
	topic      string
	sessionKey string
}

type ClientConn struct {
	ReqConn *kafkaConn
	ResConn *kafkaConn
}

var clients []kafkaConn
var server kafkaConn
var requestWorkers map[string]*kafkaConn
var responseWorkers map[string]*kafkaConn

func init() {
	server = kafkaConn{
		topic:      fmt.Sprintf("kafteria-%s", "menu"),
		sessionKey: utils.GetSessionToken(),
	}
	server.ConstructWriter()
	err := server.WriteMessage([]byte(server.topic), []byte(server.sessionKey))
	if err != nil {
		log.Fatalf("Failed to create %s", server.topic)
	}

	for i := 0; i < availableClientConns; i++ {
		sessionKey := utils.GetSessionToken()
		conn := kafkaConn{
			topic:      fmt.Sprintf("dish-%d", i),
			sessionKey: sessionKey,
		}
		conn.ConstructWriter()
		err := conn.WriteMessage([]byte(conn.topic), []byte(conn.sessionKey))
		if err != nil {
			log.Fatalf("Failed to create %s", conn.topic)
		}
		clients = append(clients, conn)
	}
}

func (kc *kafkaConn) ConstructReader() bool {
	kc.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     kc.topic,
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e6})
	return true
}

func (kc *kafkaConn) ConstructWriter() bool {
	if kc.writer != nil {
		return true
	}
	kc.writer = &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  kc.topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	messages := []kafka.Message{
		{
			Key:   []byte("menus"),
			Value: []byte("dishes"),
		},
	}
	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = kc.writer.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}
		if err != nil {
			log.Fatalf("Unexpected Error %v", err)
		}
	}
	return true
}

func (kc *kafkaConn) WriteMessage(key, value []byte) error {
	err := kc.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   key,
			Value: value,
		},
	)
	return err
}

func (kc *kafkaConn) ReadMessage(key string) (string, error) {
	var value string
	var err error
	var m kafka.Message
	for {
		m, err = kc.reader.ReadMessage(context.Background())
		if key == string(m.Key) {
			value = string(m.Value)
			break
		}
	}
	return value, err
}

func (cc *ClientConn) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {

	b, err := json.Marshal(args)
	if err != nil {
		log.Fatalf("Failed to Marshal request: %v", err)
	}
	err = cc.ReqConn.WriteMessage([]byte(cc.ReqConn.sessionKey), b)

	if err != nil {
		log.Fatalf("Failed to Write Marshaled request: %v", err)
	}

	value, err := cc.ResConn.ReadMessage(cc.ResConn.sessionKey)
	if err != nil {
		log.Fatalf("Failed to read Marshaled response: %v", err)
		return err
	}

	err = json.Unmarshal([]byte(value), &reply)
	if err != nil {
		log.Fatalf("Failed to Unmarshal response: %v", err)
		return err
	}
	cc.ResConn.Close()
	return err
}

func (cc *ClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, nil
}

func (kc *kafkaConn) Close() error {
	var err error
	if kc.reader != nil {
		err = kc.reader.Close()
		kc.reader = nil
	}
	return err
}

func Dial(methodName string) (*ClientConn, error) {
	sessionKey := utils.GetSessionToken()
	reqConn, resConn := methodNameToConn(methodName)
	reqConn.sessionKey = sessionKey
	resConn.sessionKey = sessionKey
	resConn.ConstructReader()
	err := server.WriteMessage([]byte(sessionKey), []byte(methodName))
	cc := new(ClientConn)
	cc.ReqConn = reqConn
	cc.ResConn = resConn
	return cc, err
}

func methodNameToConn(methodName string) (*kafkaConn, *kafkaConn) {
	fmt.Printf("Method = %s\n", methodName)
	reqConn, resConn := requestWorkers[methodName], responseWorkers[methodName]
	return reqConn, resConn
}

type methodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error)

type Chef struct {
	desc map[string]methodHandler
	srv  interface{}
}

func NewChef(srv interface{}, serviceDesc *grpc.ServiceDesc) *Chef {
	chef := new(Chef)
	chef.desc = make(map[string]methodHandler)
	requestWorkers = make(map[string]*kafkaConn)
	responseWorkers = make(map[string]*kafkaConn)
	i := 0
	for _, v := range serviceDesc.Methods {
		chef.desc[v.MethodName] = methodHandler(v.Handler)
		if i < availableClientConns-1 {
			requestWorkers[v.MethodName] = &clients[i]
			responseWorkers[v.MethodName] = &clients[i+1]
			i = i + 2
		}
	}
	chef.srv = srv
	return chef
}

func (c *Chef) Run() {
	server.ConstructReader()
	server.reader.SetOffsetAt(context.Background(), time.Now())
	for _, v := range requestWorkers {
		v.ConstructReader()
	}
	log.Println("Kafteria is up and running. #HappyMeals")
	for {
		m, err := server.reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Server failed to read incoming messages: %v", err)
		}
		sessionKey := string(m.Key)
		methodName := string(m.Value)
		fmt.Println(sessionKey)

		go func(sKey string, mName string) {
			handler := c.desc[mName]
			reqConn, resConn := methodNameToConn(mName)
			if reqConn == nil || resConn == nil {
				return
			}
			reqConn.sessionKey = sKey
			resConn.sessionKey = sKey
			fmt.Println(reqConn.topic)
			fmt.Println(reqConn.sessionKey)
			req, err := reqConn.ReadMessage(sKey)
			res, err := handler(c.srv, context.Background(), func(in interface{}) error {
				err := json.Unmarshal([]byte(req), in)
				fmt.Println(req)
				fmt.Println(in)
				return err
			}, nil)
			if err != nil {
				log.Fatalf("Handler function finished with an error: %v", err)
			}
			fmt.Println(res)
			d, err := json.Marshal(res)
			if err != nil {
				log.Fatalf("Failed to marshal response: %v", err)
			}
			err = resConn.WriteMessage([]byte(sessionKey), d)
			if err != nil {
				log.Fatalf("Failed to write marshaled response: %v", err)
			}
		}(sessionKey, methodName)
	}

}

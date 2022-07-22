package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/itnxs/amqp"
)

var (
	option = Option{}
	client *amqp.Client
)

// Option 配置
type Option struct {
	Host       string
	User       string
	Password   string
	Port       string
	VHost      string
	Exchange   string
	RoutingKey string
	Queue      string
	Message    string
	isLoop     bool
}

// URL 获取连接地址
func (c Option) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/", c.User, c.Password, c.Host, c.Port)
}

func init() {
	flag.StringVar(&option.Host, "h", "127.0.0.1", "rabbitMQ server address")
	flag.StringVar(&option.Port, "P", "5672", "rabbitMQ server port")
	flag.StringVar(&option.User, "u", "", "rabbitMQ server user")
	flag.StringVar(&option.Password, "p", "", "rabbitMQ server password")
	flag.StringVar(&option.VHost, "vhost", "/", "rabbitMQ vhost")
	flag.StringVar(&option.Exchange, "exchange", "test", "rabbitMQ exchange")
	flag.StringVar(&option.RoutingKey, "key", "test", "rabbitMQ routingKey")
	flag.StringVar(&option.Queue, "queue", "", "rabbitMQ queue")
	flag.StringVar(&option.Message, "message", "", "rabbitMQ message body")
	flag.BoolVar(&option.isLoop, "loop", false, "publish message loop")
	flag.Parse()

	var err error

	client, err = amqp.NewAMQPClient(option.URL(), option.VHost)
	if err != nil {
		panic(err)
	}
}

// consumer //  amqpcat -h 127.0.0.1 -u admin -p 123456 -vhost test -exchange=test -key=test -queue=test
// publiush //  amqpcat -h 127.0.0.1 -u admin -p 123456 -vhost test -exchange=test -key=test -message=test
func main() {
	if option.Message != "" {
		publish()
	} else if option.Queue != "" {
		consumer()
	} else {
		go consumer()
		publish()
	}
}

func publish() {
	ctx := context.Background()
	err := client.ExchangeDeclare(option.Exchange)
	if err != nil {
		panic(err)
	}

	var count int
	for {
		count++

		body := option.Message
		if body == "" {
			body = fmt.Sprintf("%d - %s", count, time.Now().String())
		}

		err = client.Publish(ctx, option.Exchange, option.RoutingKey, []byte(body))
		if err != nil {
			panic(err)
		}

		fmt.Println("publish: ", body)
		if !option.isLoop {
			break
		}
		time.Sleep(time.Second)
	}
}

func consumer() {
	err := client.QueueDeclare(option.Queue)
	if err != nil {
		panic(err)
	}

	err = client.QueueBind(option.Exchange, option.Queue, option.RoutingKey)
	if err != nil {
		panic(err)
	}

	err = client.Consume(option.Queue, func(body []byte) error {
		fmt.Println("consume: ", string(body))
		return nil
	})
	if err != nil {
		panic(err)
	}
}

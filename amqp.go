package amqp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// ProducerMessage producer message
type ProducerMessage struct {
	Exchange string
	Key      string
	Body     []byte
}

// Client amqp client
type Client struct {
	url, host    string
	connection   *amqp.Connection
	channel      *amqp.Channel
	logger       *logrus.Logger
	connectWait  sync.WaitGroup
	connectLock  int32
	connectCount int32
}

// NewAMQPClient 新建amqp client
func NewAMQPClient(url, vHost string) (*Client, error) {
	c := &Client{
		url:          url,
		host:         vHost,
		logger:       logrus.New(),
	}

	if err := c.reconnect(); err != nil {
		return nil, errors.Wrap(err, "rabbitMQ connect")
	}

	return c, nil
}

func (c *Client) getClient() (*amqp.Connection, error) {
	var err error

	if c.connection == nil {
		c.connection, err = amqp.DialConfig(c.url, amqp.Config{Vhost: c.host})
		if err != nil {
			return nil, errors.Wrap(err, "connection dial")
		}
	}

	return c.connection, nil
}

func (c *Client) getChannel() (*amqp.Channel, error) {
	var conn *amqp.Connection
	var err error

	if c.channel == nil {
		conn, err = c.getClient()
		if err != nil {
			return nil, err
		}

		c.channel, err = conn.Channel()
		if err != nil {
			return nil, errors.Wrap(err, "connection channel")
		}
	}

	return c.channel, nil
}

func (c *Client) reconnect() error {
	c.connectWait.Add(1)
	defer c.connectWait.Done()

	if atomic.AddInt32(&c.connectLock, 1) > 1 {
		return nil
	}
	defer atomic.StoreInt32(&c.connectLock, 0)

	count := atomic.AddInt32(&c.connectCount, 1)
	c.logger.WithField("connectCount", count).Info("rabbitMQ reconnect")

	func() {
		defer func() {
			if evr := recover(); evr != nil {
				c.logger.Error(evr)
			}
		}()

		if c.channel != nil {
			_ = c.channel.Close()
		}

		if c.connection != nil && !c.connection.IsClosed() {
			_ = c.connection.Close()
		}
	}()

	c.channel = nil
	c.connection = nil

	_, err := c.getClient()
	if err != nil {
		return err
	}

	_, err = c.getChannel()
	if err != nil {
		return err
	}

	return nil
}

// Publish 发布消息
func (c *Client) Publish(ctx context.Context, exchange, key string, body []byte) error {
	var errorCount int

	m := &ProducerMessage{
		Exchange: exchange,
		Key:      key,
		Body:     body,
	}

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		default:
		}

		c.connectWait.Wait()

		channel, err := c.getChannel()
		if err == nil {
			err = channel.Publish(m.Exchange, m.Key, false, false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					Timestamp:    time.Now(),
					Body:         m.Body,
				},
			)
			if err == nil {
				return nil
			}
		}

		errorCount++
		if errorCount >= 3 {
			c.logger.WithFields(logrus.Fields{
				"exchange": exchange,
				"key":      key,
				"body":     string(body),
			}).WithError(err).Error("rabbitMQ publish message error")

			return errors.WithStack(err)
		} else if err := c.reconnect(); err != nil {
			return errors.Wrap(err, "rabbitMQ reconnect")
		}
	}
}

// Consume 消费消息
func (c *Client) Consume(queue string, handler func(body []byte) error) error {
	channel, err := c.getChannel()
	if err != nil {
		return err
	}

	messages, err := channel.Consume(queue, "", false, true, false, false, nil)
	if err != nil {
		return errors.Wrap(err, "channel consume")
	}

	for {
		select {
		case m := <-messages:
			if err := handler(m.Body); err != nil {
				return errors.Wrap(err, "consume handler")
			}
			if err := channel.Ack(m.DeliveryTag, false); err != nil {
				return errors.Wrap(err, "consume ack")
			}
		}
	}
}

// ExchangeDeclare 创建交换机
func (c *Client) ExchangeDeclare(exchanges ...string) error {
	channel, err := c.getChannel()
	if err != nil {
		return err
	}

	for _, exchange := range exchanges {
		err := channel.ExchangeDeclare(
			exchange,            // name
			amqp.ExchangeDirect, // type
			true,                // durable
			false,               // auto delete
			false,               // internal
			false,               // noWait
			nil,                 // arguments
		)
		if err != nil {
			return errors.Wrapf(err, "channel declare %s", exchange)
		}
	}
	return nil
}

// QueueDeclare 创建队列
func (c *Client) QueueDeclare(queues ...string) error {
	channel, err := c.getChannel()
	if err != nil {
		return err
	}

	for _, queue := range queues {
		_, err := channel.QueueDeclare(
			queue, // name
			true,  // durable
			false, // auto delete
			false, // exclusive
			false, // noWait
			nil,   // arguments
		)
		if err != nil {
			return errors.Wrapf(err, "queue declare %s", queue)
		}
	}
	return nil
}

// QueueBind 绑定队列
func (c *Client) QueueBind(exchange, queue, key string) error {
	channel, err := c.getChannel()
	if err != nil {
		return err
	}

	err = channel.QueueBind(queue, key, exchange, false, nil)
	return errors.WithStack(err)
}

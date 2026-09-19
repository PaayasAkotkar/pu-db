package pudb

import (
	"context"
	"fmt"
	"log"
	"pu/app/common"
	vadb "va/app/core"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

type IResult struct {
	Ready bool
	Pull  *vadb.IPull
}

type Handler func(data *IResult)

const (
	MaxPendingMessages = 120
)

func createURL(c Class, bucket, branch, object string) string {
	switch c {
	case RAM:
		return fmt.Sprintf("non-persistent://%s/%s/%s", bucket, branch, object)
	case DISK:
		return fmt.Sprintf("persistent://%s/%s/%s", bucket, branch, object)
	}
	return common.StringSentinel
}

func (p *IPubDBAdmin) createObject(ctx context.Context, domain string, c *ICreateObject) error {
	topic := fmt.Sprintf("%s://%s/%s/%s", domain, c.Bucket, c.Branch, c.Object)
	partitions := c.Partition

	name, err := utils.GetTopicName(topic)
	if err != nil {
		return err
	}

	if !c.Update {
		if err = p.admin.Topics().CreateWithContext(ctx, *name, partitions); err != nil {
			return err
		}
	}

	if c.Cfg != nil {
		p.admin.Topics().SetMaxConsumersWithContext(ctx, *name, c.Cfg.Users)
		p.admin.Topics().SetMaxProducersWithContext(ctx, *name, c.Cfg.Authors)
		p.admin.Topics().SetMaxMessageSizeWithContext(ctx, *name, c.Cfg.PageSize)
		p.admin.Topics().SetMessageTTLWithContext(ctx, *name, int(c.Cfg.SaleTime))
	}

	if c.Update {
		if err = p.admin.Topics().UpdateWithContext(ctx, *name, partitions); err != nil {
			return err
		}
	}
	return nil
}

// subscribeReady subscribes to a topic and reports when the broker consumer is ready.
func (p *IPuDB) subscribeReady(
	ctx context.Context,
	co []FnConsumerOption,
	propertyControl func(properties map[string]string) error, c Class,
	bucket, branch, object, bookmark string,
) (chan *IResult, func()) {
	out := make(chan *IResult, 1222)

	url := createURL(c, bucket, branch, object)
	if url == common.StringSentinel {
		close(out)
		return out, func() {}
	}

	_co := pulsar.ConsumerOptions{Topic: url, SubscriptionName: bookmark, Type: pulsar.Failover}
	for _, fn := range co {
		fn(&_co)
	}
	_co.Topic, _co.SubscriptionName = url, bookmark

	consumer, err := p.cli.Subscribe(_co)
	if err != nil {
		log.Printf("subscribe to %s failed: %v", url, err)
		close(out)
		return out, func() {}
	}
	_ctx, stop := context.WithCancel(ctx)

	listen := func(consumer pulsar.Consumer) {
		for {
			message, err := consumer.Receive(_ctx)
			if err != nil {
				return
			}

			if propertyControl != nil {
				if err := propertyControl(message.Properties()); err != nil {
					log.Println(err)
					consumer.Nack(message)
					continue
				}
			}

			if message.Properties()[PBookmark] != bookmark {
				if err := consumer.Ack(message); err != nil {
					log.Printf("ack unrelated message failed: %v", err)
				}
				continue
			}

			result := &IResult{
				Ready: true,
				Pull: &vadb.IPull{
					Bucket: bucket, Branch: branch, Object: object,
					Data: string(message.Payload()), Fresh: true, Mode: vadb.MAP,
				},
			}
			select {
			case out <- result:
			case <-_ctx.Done():
				return
			}

			if err := consumer.Ack(message); err != nil {
				log.Printf("ack message failed: %v", err)
			}
		}
	}

	go func() {
		defer close(out)

		for {
			listen(consumer)
			consumer.Close()

			if _ctx.Err() != nil {
				return // stop() or ctx: intended, stay quiet
			}
			log.Printf("consumer for %s closed unexpectedly, resubscribing", url)

			for {

				if consumer, err = p.cli.Subscribe(_co); err == nil {
					break
				}
				log.Printf("resubscribe %s failed: %v", url, err)
			}
		}
	}()

	return out, stop
}
func (p *IPuDB) found(c Class, bucket, branch, object, bookmark string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.config.Cache {
		if e.Class == c && e.Bucket == bucket && e.Branch == branch &&
			e.Object == object && e.Bookmark == bookmark {
			return true
		}
	}
	return false
}

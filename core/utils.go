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
) chan *IResult {
	out := make(chan *IResult, 1)

	go func() {
		defer close(out)

		url := createURL(c, bucket, branch, object)
		if url == common.StringSentinel {
			return
		}
		_co := pulsar.ConsumerOptions{
			Topic:                       url,
			SubscriptionName:            bookmark,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			Type:                        pulsar.Failover,
		}
		for _, fn := range co {
			fn(&_co)
		}
		_co.Topic = url
		_co.SubscriptionName = bookmark

		consumer, err := p.cli.Subscribe(_co)
		if err != nil {
			log.Printf("subscribe to %s failed: %v", url, err)
			return
		}
		defer consumer.Close()

		for {
			message, err := consumer.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf("receive from %s failed: %v", url, err)
				return
			}

			if message == nil {
				continue
			}

			if err := propertyControl(message.Properties()); err != nil {
				log.Println(err)
				continue
			}

			b := message.Properties()[PBookmark]
			log.Println("goin well...")

			if b != bookmark {
				log.Println("bookmark not matched...")
				if err := consumer.Ack(message); err != nil {
					log.Printf("ack unrelated message failed: %v", err)
					return
				}

				continue
			}

			result := &IResult{
				Ready: true,
				Pull: &vadb.IPull{
					Bucket: bucket,
					Branch: branch,
					Object: object,
					Data:   string(message.Payload()),
					Fresh:  true,
					Mode:   vadb.MAP,
				},
			}

			select {
			case <-ctx.Done():
				return

			case out <- result:
			}

			if err := consumer.Ack(message); err != nil {
				log.Printf("ack message failed: %v", err)
				return
			}
		}
	}()

	return out
}

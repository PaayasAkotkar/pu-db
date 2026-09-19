package pudb

import (
	"context"
	"log"
	vadb "va/app/core"
	vasdk1 "va/app/sdk"

	"github.com/apache/pulsar-client-go/pulsar"
)

type FnProducerOption func(*pulsar.ProducerOptions)
type FnProducerMessage func(*pulsar.ProducerMessage)
type FnConsumerOption func(*pulsar.ConsumerOptions)

// Publish send the data
func (p *IPuDB) Publish(ctx context.Context, c Class, bucket, branch, object, bookmark string, properties map[string]string, value []byte, po []FnProducerOption, pm []FnProducerMessage) error {
	url := createURL(c, bucket, branch, object)
	if properties == nil {
		properties = make(map[string]string, 1)
	}
	properties[PBookmark] = bookmark
	opt := pulsar.ProducerOptions{
		Topic:              url,
		Properties:         properties,
		MaxPendingMessages: MaxPendingMessages,
	}
	for _, fn := range po {
		fn(&opt)
	}
	opt.Topic = url // shadowing

	pr, err := p.cli.CreateProducer(opt)

	if err != nil {
		return err
	}
	defer pr.Close()

	mp := pulsar.ProducerMessage{
		Payload:    value,
		Properties: properties,
	}

	for _, fn := range pm {
		fn(&mp)
	}
	mp.Payload = value
	_, err = pr.Send(ctx, &mp)
	return err
}

type IBroadcast struct {
	Bucket, Branch, Object string
	Class                  Class
	Bookmark               string // subscription
	Property               map[string]string
	Data                   []byte
	FnPO                   []FnProducerOption
	FnPm                   []FnProducerMessage
}

// Broadcast publihse list of messages
func (p *IPuDB) Broadcast(ctx context.Context, c []*IBroadcast) {
	go func() {
		for _, r := range c {
			//ub := fmt.Sprintf("%s_%d", r.Bookmark, i)
			ub := r.Bookmark

			if err := p.Publish(ctx, r.Class, r.Bucket, r.Branch, r.Object, ub, r.Property, r.Data, r.FnPO, r.FnPm); err != nil {
				log.Printf("broadcast %s/%s/%s failed: %v", r.Bucket, r.Branch, r.Object, err)
				continue
			}
			log.Printf("broadcast %s/%s/%s published", r.Bucket, r.Branch, r.Object)
		}
	}()
}

// Subscribe returns the channel
// note: it is like pooling so yeah be-careful with it
// note: without valkey
func (p *IPuDB) Subscribe(ctx context.Context, co []FnConsumerOption, propertyControl func(properties map[string]string) error, c Class, bucket, branch, object, bookmark string) chan *IResult {
	return p.subscribeReady(ctx, co, propertyControl, c, bucket, branch, object, bookmark)
}

// GoMonitor uses the pushed cache to pull the result
// note: we use the heap one time set and ready to go
// propertyControl: control the writing of property one time than modfiy each time
func (p *IPuDB) GoMonitor(ctx context.Context, co []FnConsumerOption, propertyControl func(properties map[string]string) error, handler Handler) {
	for _, cache := range p.config.Cache {
		param := cache

		go func(param *ICache) {
			url := createURL(
				param.Class,
				param.Bucket,
				param.Branch,
				param.Object,
			)

			po := pulsar.ConsumerOptions{
				Topic:            url,
				SubscriptionName: param.Bookmark,
				Type:             pulsar.Failover,
			}

			for _, fn := range co {
				fn(&po)
			}
			po.Topic = url
			po.SubscriptionName = param.Bookmark

			consumer, err := p.cli.Subscribe(po)

			if err != nil {
				log.Printf(
					"subscribe to %s: %v",
					param.Object,
					err,
				)
				return
			}
			defer consumer.Close()

			for {
				message, err := consumer.Receive(ctx)

				if err := propertyControl(message.Properties()); err != nil {
					log.Println(err)
					continue
				}

				bookmark := message.Properties()[PBookmark]
				log.Println("going well...")

				if bookmark != param.Bookmark {
					log.Println("yes:= ", string(message.Payload()))
					if err := consumer.Ack(message); err != nil {
						log.Println("ack unrelated message:", err)
						return
					}
					continue
				}

				if err != nil {
					if ctx.Err() != nil {
						return
					}

					log.Println("pulsar receive:", err)
					return
				}

				p.va.TPublish(ctx, vadb.MAP, &vadb.IPush{
					Bucket: param.Bucket,
					Branch: param.Branch,
					Object: param.Object,
					Data:   string(message.Payload()),
				})

				if err := consumer.Ack(message); err != nil {
					log.Println("pulsar ack:", err)
					return
				}
			}
		}(param)

		go func(param *ICache) {
			results := p.va.TSubscribe(
				ctx,
				p.config.Outcomes,
				vadb.MAP,
				param.Bucket,
				param.Branch,
				param.Object,
			)

			for {
				select {
				case <-ctx.Done():
					return

				case result, ok := <-results:
					if !ok {
						return
					}

					if result == nil {
						continue
					}

					handler(&IResult{
						Pull:  result,
						Ready: true,
					})
				}
			}
		}(param)
	}
}

// AppendCache push the cache
func (p *IPuDB) AppendCache(key string, c *ICache) {
	p.mu.Lock()
	p.config.Cache[key] = c
	p.mu.Unlock()
}

// SetCache set's the default hash
func (p *IPuDB) SetCache(cache map[string]*ICache) {
	p.mu.Lock()
	p.config.Cache = cache
	p.mu.Unlock()
}

func (p *IPuDB) VDB() *vasdk1.IVaDB {
	return p.va
}

// ClearCache clear the heap
func (p *IPuDB) ClearCache() {
	p.mu.Lock()
	clear(p.config.Cache)
	p.mu.Unlock()
}

// Admin get the admin for current file
func (p *IPuDB) Admin() *IPubDBAdmin {
	return p.admin
}

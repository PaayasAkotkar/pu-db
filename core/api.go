package pudb

import (
	"context"
	"fmt"
	"log"
	"pu/app/common"
	"sync"
	vadb "va/app/core"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Publish send the data
func (p *IPuDB) Publish(ctx context.Context, c Class, bucket, branch, object, bookmark string, propety map[string]string, value []byte) error {
	url := createURL(c, bucket, branch, object)
	pr, err := p.cli.CreateProducer(pulsar.ProducerOptions{
		Topic:              url,
		Name:               bookmark,
		Properties:         propety,
		MaxPendingMessages: MaxPendingMessages,
	})
	if err != nil {
		return err
	}
	defer pr.Close()

	_, err = pr.Send(ctx, &pulsar.ProducerMessage{
		Payload:    value,
		Properties: propety,
	})
	return err
}

type IBroadcast struct {
	Bucket, Branch, Object string
	Class                  Class
	Bookmark               string // subscription
	Property               map[string]string
	Data                   []byte
}

// Broadcast publihse list of messages
func (p *IPuDB) Broadcast(ctx context.Context, c []*IBroadcast) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for _, r := range c {
			if err := p.Publish(ctx, r.Class, r.Bucket, r.Branch, r.Object, r.Bookmark, r.Property, r.Data); err != nil {
				log.Printf("broadcast %s/%s/%s failed: %v", r.Bucket, r.Branch, r.Object, err)
				continue
			}
			log.Printf("broadcast %s/%s/%s published", r.Bucket, r.Branch, r.Object)
		}
	}()
}

// Subscribe returns the channel
// note: it is like pooling so yeah be-careful with it
func (p *IPuDB) Subscribe(ctx context.Context, c Class, bucket, branch, object, bookmark string) chan *IResult {
	out, _ := p.SubscribeReady(ctx, c, bucket, branch, object, bookmark)
	return out
}

// SubscribeReady subscribes to a topic and reports when the broker consumer is ready
func (p *IPuDB) SubscribeReady(ctx context.Context, c Class, bucket, branch, object, bookmark string) (chan *IResult, <-chan error) {
	out := make(chan *IResult, 1)
	ready := make(chan error, 1)
	go func() {
		defer close(out)
		url := createURL(c, bucket, branch, object)
		if url == common.StringSentinel {
			ready <- fmt.Errorf("invalid topic class %q", c)
			return
		}
		consumer, err := p.cli.Subscribe(pulsar.ConsumerOptions{
			Topic:            url,
			SubscriptionName: bookmark,
		})

		if err != nil {
			ready <- err
			log.Println(err)
			return
		}
		ready <- nil

		defer consumer.Close()

		for {
			message, err := consumer.Receive(ctx)
			if err != nil {
				log.Println(err)
				return
			}
			consumer.Ack(message)
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
		}
	}()
	return out, ready
}

// GoMonitor uses the pushed cache to pull the result
// note: we use the heap one time set and ready to go
func (p *IPuDB) GoMonitor(ctx context.Context, handler Handler) {
	var monitors sync.WaitGroup
	for _, param := range p.config.Cache {
		monitors.Add(1)
		go func(param *ICache) {
			defer monitors.Done()
			url := createURL(param.Class, param.Bucket, param.Branch, param.Object)
			consumer, err := p.cli.Subscribe(pulsar.ConsumerOptions{
				Topic:            url,
				SubscriptionName: param.Bookmark,
			})
			if err != nil {
				log.Printf("subscribe to %s: %v", param.Object, err)
				return
			}
			defer consumer.Close()

			for {
				message, err := consumer.Receive(ctx)
				if err != nil {
					return
				}
				consumer.Ack(message)
				handler(&IResult{
					Ready: true,
					Pull: &vadb.IPull{
						Bucket: param.Bucket,
						Branch: param.Branch,
						Object: param.Object,
						Data:   string(message.Payload()),
						Fresh:  true,
						Mode:   vadb.MAP,
					},
				})
			}

		}(param)
	}
	monitors.Wait()
}

// AppendCache push the cache
func (p *IPuDB) AppendCache(key string, c *ICache) {
	p.mu.Lock()
	p.config.Cache[key] = c
	p.mu.Unlock()
}

// SetCache set's the default hash
// note: I personally recommend using the AppendCache
func (p *IPuDB) SetCache(cache map[string]*ICache) {
	p.mu.Lock()
	p.config.Cache = cache
	p.mu.Unlock()
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

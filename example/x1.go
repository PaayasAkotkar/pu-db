package example

import (
	"context"
	"log"
	pudb "pu/app/core"
	"pu/app/pencil"
	"sync"
	"time"
	vadb "va/app/core"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/valkey-io/valkey-go"
)

// Multi represnets all the example in once
// note: this is a bulk example meaning all the api is uesd in once
// make sure to comment some in-order to view result properly
// note: example is written by vs-code copilet so shotout to that
func Multi() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	valkeyClient, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})

	if err != nil {
		log.Fatal("create Valkey client: ", err)
	}
	defer valkeyClient.Close()

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		log.Fatal("create Pulsar client: ", err)
	}
	defer pulsarClient.Close()

	adminClient, err := pulsaradmin.NewClient(&config.Config{
		WebServiceURL: "http://localhost:8081",
	})

	if err != nil {
		log.Fatal("create Pulsar admin client: ", err)
	}

	db := pudb.New(pulsarClient, adminClient, vadb.IConfig{
		Cli: valkeyClient,
		Set: &vadb.ISettings{TTL: 12 * time.Second},
	}, 100)

	p := pencil.New()
	admin := db.Admin()
	bucket, branch := "example_bucket", "example_branch"
	object, monitorObject := "example_object", "monitor_object"
	class := pudb.RAM
	var wg sync.WaitGroup
	wg.Add(2)
	defer pulsarClient.Close()

	// Admin lifecycle: bucket -> branch -> objects
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	must(admin.PushBucket(ctx, &pudb.ICreateBucket{Bucket: bucket}))
	must(admin.PushBranch(ctx, &pudb.ICreateBranch{Bucket: bucket, Branch: branch}))
	must(admin.PushObject(ctx, &pudb.ICreateObject{Bucket: bucket, Branch: branch, Object: object, Class: class}))
	must(admin.PushObject(ctx, &pudb.ICreateObject{Bucket: bucket, Branch: branch, Object: monitorObject, Class: class}))

	db.SetCache(map[string]*pudb.ICache{
		"monitor": {
			Bucket: bucket, Branch: branch, Object: monitorObject,
			Class: class, Bookmark: "monitor_bookmark",
		},
	})

	db.GoMonitor(ctx, []pudb.FnConsumerOption{
		func(co *pulsar.ConsumerOptions) {
		},
	}, func(properties map[string]string) error {
		p.Pen(pencil.Orange, "properties: ", properties)
		return nil
	}, func(result *pudb.IResult) {
		if result != nil && result.Ready {
			p.Pen(pencil.Yellow, "monitor received data: ", result.Pull.Data)
		}
	})

	//	ch, stop := db.Subscribe(
	//		ctx,
	//		nil,
	//		nil,
	//		class,
	//		bucket,
	//		branch,
	//		monitorObject,
	//		"subscriber_bookmark",
	//	)
	//
	//	go func() {
	//		for {
	//			select {
	//			case result, ok := <-ch:
	//				if !ok {
	//					return
	//				}
	//
	//				if result != nil && result.Pull != nil {
	//					p.Pen(
	//						pencil.Green,
	//						"sub receive: ",
	//						result.Pull.Data,
	//					)
	//				}
	//
	//			case <-ctx.Done():
	//				stop()
	//				return
	//			}
	//		}
	//	}()
	//
	//	if err := db.Publish(ctx, class, bucket, branch, monitorObject, "subscriber_bookmark", nil, []byte("yo"), nil, nil); err != nil {
	//		panic(err)
	//	}

	db.Broadcast(ctx, []*pudb.IBroadcast{
		{
			Bucket: bucket, Branch: branch, Object: monitorObject, Class: class,
			Bookmark: "monitor_bookmark", Data: []byte("broadcast message1"),
			FnPO: []pudb.FnProducerOption{},
			FnPm: []pudb.FnProducerMessage{},
		},
		{
			Bucket: bucket, Branch: branch, Object: monitorObject, Class: class,
			Bookmark: "monitor_bookmark", Data: []byte("broadcast message2"),
		},
	})
	wg.Wait()
}

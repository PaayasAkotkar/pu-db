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
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/valkey-io/valkey-go"
)

// Multi represnets all the example in once
// note: this is a bulk example meaning all the api is uesd in once
// make sure to comment some in-order to view result properly
// note: example is written by vs-code copilet so shotout to that
func Multi() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// Admin lifecycle: bucket -> branch -> objects.
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}
	must(admin.PushBucket(ctx, &pudb.ICreateBucket{Bucket: bucket}))
	must(admin.PushBranch(ctx, &pudb.ICreateBranch{Bucket: bucket, Branch: branch}))
	must(admin.PushObject(ctx, &pudb.ICreateObject{Bucket: bucket, Branch: branch, Object: object, Class: class}))
	must(admin.PushObject(ctx, &pudb.ICreateObject{Bucket: bucket, Branch: branch, Object: monitorObject, Class: class}))
	log.Printf("buckets: %v", admin.PullBuckets(ctx))
	log.Printf("branches: %v", admin.PullBranches(ctx, bucket))
	log.Printf("branch topics: %v", admin.GetBranch(ctx, bucket, branch))
	log.Printf("objects: %v", admin.PullObjects(ctx, bucket, branch))
	log.Printf("bucket exists: %t", admin.DoesBucketExists(ctx, bucket))
	log.Printf("branch exists: %t", admin.DoesBranchExists(ctx, bucket, branch))
	log.Printf("object exists: %t", admin.DoesObjectExists(ctx, class, bucket, branch, object))

	db.SetCache(map[string]*pudb.ICache{
		"monitor": {
			Bucket: bucket, Branch: branch, Object: monitorObject,
			Class: class, Bookmark: "monitor_bookmark",
		},
		"subscriber": {
			Bucket: bucket, Branch: branch, Object: object,
			Class: class, Bookmark: "subscriber_bookmark",
		},
	})
	monitorDone := make(chan struct{}, 1)
	broadcastDone := make(chan struct{}, 1)
	subscriberDone := make(chan struct{}, 1)
	var monitorOnce sync.Once
	var subscriberOnce sync.Once

	go func() {
		db.GoMonitor(ctx, func(result *pudb.IResult) {
			if result != nil && result.Ready {
				p.Pen(pencil.Yellow, "monitor received: ", result.Pull)
				monitorOnce.Do(func() { monitorDone <- struct{}{} })
			}
		})
	}()

	broadcastSubscription, broadcastReady := db.SubscribeReady(ctx, class, bucket, branch, monitorObject, "broadcast_subscriber")
	go func() {
		result, ok := <-broadcastSubscription
		if ok && result != nil && result.Ready {
			p.Pen(pencil.Green, "broadcast received: ", result.Pull)
			broadcastDone <- struct{}{}
		}
	}()

	subscription, subscriberReady := db.SubscribeReady(ctx, class, bucket, branch, object, "subscriber_bookmark")
	for name, ready := range map[string]<-chan error{
		"broadcast":  broadcastReady,
		"subscriber": subscriberReady,
	} {
		select {
		case err := <-ready:
			if err != nil {
				log.Fatal(name, " consumer: ", err)
			}
		case <-ctx.Done():
			log.Fatal(name, " consumer readiness timed out: ", ctx.Err())
		}
	}

	go func() {
		defer func() { subscriberOnce.Do(func() { subscriberDone <- struct{}{} }) }()
		select {
		case result, ok := <-subscription:
			if ok && result != nil {
				p.Pen(pencil.Green, "subscribed: ", result.Pull)
				subscriberDone <- struct{}{}
			}
		case <-ctx.Done():
			log.Print("subscription timed out: ", ctx.Err())
		}
	}()

	must(db.Publish(ctx, class, bucket, branch, object, "publisher", nil, []byte("published message")))

	db.Broadcast(ctx, []*pudb.IBroadcast{
		{
			Bucket: bucket, Branch: branch, Object: monitorObject, Class: class,
			Bookmark: "monitor_publisher", Data: []byte("broadcast message1"),
		},
		{
			Bucket: bucket, Branch: branch, Object: monitorObject, Class: class,
			Bookmark: "monitor_publisher", Data: []byte("broadcast message2"),
		},
	})

	select {
	case <-broadcastDone:
	case <-ctx.Done():
		log.Print("broadcast timed out: ", ctx.Err())
	}
	select {
	case <-subscriberDone:
	case <-ctx.Done():
	}
	select {
	case <-monitorDone:
	case <-ctx.Done():
	}

	if err := admin.ReadObjectStats(ctx, bucket, branch, object, func(stats utils.TopicStats) {
		log.Printf("object stats: %+v", stats)
	}); err != nil {
		p.Pen(pencil.Red, "read stats: ", err)
	}

	db.ClearCache()

	p.Pen(pencil.Blue, "before release: ")
	for _, b := range db.Admin().PullBuckets(ctx) {
		for _, br := range db.Admin().PullBranches(ctx, b) {
			for _, o := range db.Admin().PullObjects(ctx, b, br) {
				p.Pen(pencil.Green, "bucket: ", b)
				p.Pen(pencil.Orange, "branch: ", br)
				p.Pen(pencil.Violet, "object: ", o)
			}
		}
	}
	p.Pen(pencil.Red, "***end***")
	db.Admin().Release(ctx)

	p.Pen(pencil.Blue, "after release: ")
	for _, b := range db.Admin().PullBuckets(ctx) {
		for _, br := range db.Admin().PullBranches(ctx, b) {
			for _, o := range db.Admin().PullObjects(ctx, b, br) {
				p.Pen(pencil.Green, "bucket: ", b)
				p.Pen(pencil.Orange, "branch: ", br)
				p.Pen(pencil.Violet, "object: ", o)
			}
		}
	}
	p.Pen(pencil.Red, "***end***")
	cancel()
}

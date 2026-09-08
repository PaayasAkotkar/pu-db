package pudb

import (
	"sync"
	"time"
	vadb "va/app/core"
	vasdk1 "va/app/sdk"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin"
)

type IPuDB struct {
	cli    pulsar.Client
	admin  *IPubDBAdmin
	va     *vasdk1.IVaDB
	config *IConfig
	mu     sync.Mutex
	wg     sync.WaitGroup
}

type IConfig struct {
	Cache map[string]*ICache // key:params_required_to_build_url

	AutoRelease bool // if auto-release once the message is reached we delete the topic from the disk

}
type ICache struct {
	Bucket, Branch, Object string
	Class                  Class
	Bookmark               string // subscription
	Property               map[string]string
}

func New(cli pulsar.Client, ad pulsaradmin.Client, c vadb.IConfig, n int) *IPuDB {
	return &IPuDB{
		cli: cli,
		admin: &IPubDBAdmin{
			admin: ad,
			cfg: &IAdminConfig{
				RetryAttempts: 3,
				Delay:         time.Second,
			},
		},
		va: vasdk1.New(c, n),
		config: &IConfig{
			Cache: make(map[string]*ICache),
		},
	}
}

func ma() {
	adm, err := pulsaradmin.NewClient(&pulsaradmin.Config{})
	if err != nil {
		panic(err)
	}
	adm.Namespaces().CreateNamespace("")
	cli, err := pulsar.NewClient(pulsar.ClientOptions{})
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.Subscribe(pulsar.ConsumerOptions{})
	cli.CreateReader(pulsar.ReaderOptions{})
}

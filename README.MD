## Pu-DB 
Pu-DB or Pulsar Database expands the apache pulsar go-client & provides OBB format writing using an internal pub-sub system to push the message in cache

provided by va-db

## Architecture 🔥
```
based on Bucket,Branch,Object infrasturcture
Each call received is first pushed into cache
and than returning the result
```

## Example 😄
```golang 
// push the object with easy
	db.Admin().PushObject(ctx, &pudb.ICreateObject{Bucket: "rockstar", Branch: "action", Object: "gta-vi", Class: pudb.DISK})
    // set the cache one time in order to auto fetch the result ....
db.SetCache(map[string]*pudb.ICache{
		"monitor": {
			Bucket: bucket, Branch: branch, Object: monitorObject,
			Class: class, Bookmark: "youtube",
		},
		"subscriber": {
			Bucket: bucket, Branch: branch, Object: object,
			Class: class, Bookmark: "ps5",
		},
	})
// 2 ways to monitor message(s):

// 1. Auto get
db.GoMonitor(ctx, []pudb.FnConsumerOption{
		func(co *pulsar.ConsumerOptions) {
// add as many options in your way

				},}, 
				func(properties map[string]string) error {
					// read property for each fetch message
		return nil
	},
				func(result *pudb.IResult) {
			if result != nil && result.Ready {
				p.Pen(pencil.Yellow, "received: ", result.Pull)
			}
		})

// 2. channel
ch:=db.Subscribe(ctx, class, bucket, branch, monitorObject, "youtube")


// 2 ways to send message:

// 1. single messsage
db.Publish(ctx, class, bucket, branch, object, "ps5", nil, []byte("releases on nov 19 2026"))

// 2. multiple messages
	db.Broadcast(ctx, []*pudb.IBroadcast{
		{
			Bucket: bucket, Branch: branch, Object: monitorObject, Class: class,
			Bookmark: "youtube", Data: []byte("offical teaser"),
						FnPO: []pudb.FnProducerOption{
							// push custom options in better way
						},
			FnPm: []pudb.FnProducerMessage{
											// push custom options in better way
			},

		},
		{
			Bucket: bucket, Branch: branch, Object: monitorObject, Class: class,
			Bookmark: "youtube", Data: []byte("offical trailer"),
		},
	})


```
## Why use Pu-DB 🤨?
| Before                                                             | After                                                                                               |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| full url to pull the result without def knowledge                  | now use the bucket,branch,object and a bookmark to address the result & pull it                     |
| No clear cache as a microservice to load the result in cache first | now auto pull the result & returns the result                                                       |
| subscribe to many topics at once but has to manage the async call  | set the structure one time and simply use the GoMonitor without needing to implement complex result |
| manage producer for each data puublish                             | simply add the config just like any other publish func                                              |

## Note
I am currently working more on the pulsar usage; I wrote what I feel like easy for me to use in one of my development project, I hope the same for you

## Note
do not use subscription and go-monitor for same url

## Changes
- go-monitor now supports the cache now perfectly
- bookmark is taken seriously now

Happy coding 🤗
- va-db link: https://github.com/PaayasAkotkar/va-db

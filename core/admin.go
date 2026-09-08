package pudb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/codeGROOVE-dev/retry"
)

type IPubDBAdmin struct {
	admin pulsaradmin.Client
	cfg   *IAdminConfig
	mu    sync.Mutex
}
type IAdminConfig struct {
	AllowRetry    bool
	RetryAttempts uint
	Delay         time.Duration
	DelayType     func(attempt uint, err error, config *retry.Config) time.Duration
}

// structs analogy bucket->branch->[create]
// up-until now you following the url stuff; now simply pass the bucket,branch and object

type ICreateObject struct {
	Bucket, Branch, Object string
	Partition              int
	Class                  // DISK|RAM follows analogy persistent|non-persistent
	Update                 bool
	Cfg                    *ICreateConfig
}

type ICreateConfig struct {
	Users, Authors int           // analogy as consumers|producers
	PageSize       int           // payload size or message size
	SaleTime       time.Duration // or ttl
	Subscription   int
}
type ICreateBranch struct {
	Bucket, Branch string
	Cfg            *ICreateConfig
}

type ICreateBucket struct {
	Bucket   string
	Roles    []string
	Clusters []string
	Update   bool
}

type Class string

const (
	DISK Class = "DISK"
	RAM  Class = "RAM"
)

// PushBucket creates new logical tenant if not a update request
func (p *IPubDBAdmin) PushBucket(ctx context.Context, c *ICreateBucket) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c == nil {
		return fmt.Errorf("[empty struct]")
	}
	if len(c.Clusters) == 0 {
		c.Clusters = []string{"standalone"}
	}
	if !p.DoesBucketExists(ctx, c.Bucket) && !c.Update {
		err := p.admin.Tenants().CreateWithContext(ctx, utils.TenantData{
			Name:            c.Bucket,
			AdminRoles:      c.Roles,
			AllowedClusters: c.Clusters,
		})
		if err != nil {
			return fmt.Errorf("cannot push bucket %v", err)
		}
	}
	if c.Update {
		err := p.admin.Tenants().UpdateWithContext(ctx, utils.TenantData{
			Name:            c.Bucket,
			AdminRoles:      c.Roles,
			AllowedClusters: c.Clusters,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// PushBranch append the namesapce
// note: bucket & branch is mandtaroy
func (p *IPubDBAdmin) PushBranch(ctx context.Context, c *ICreateBranch) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c == nil {
		return fmt.Errorf("[empty struct]")
	}
	if !p.DoesBucketExists(ctx, c.Bucket) {
		return fmt.Errorf("[bucket with name %s not found]", c.Bucket)
	}

	if p.DoesBranchExists(ctx, c.Bucket, c.Branch) {
		log.Printf("[branch %s already exists]", c.Branch)
		return nil
	}

	url, err := utils.GetNameSpaceName(c.Bucket, c.Branch)
	if err != nil {
		return err
	}
	if err = p.admin.Namespaces().CreateNamespaceWithContext(ctx, url.String()); err != nil {
		return err
	}

	if c.Cfg != nil {
		return retry.Do(
			func() error {
				name, err := utils.GetNameSpaceName(c.Bucket, c.Branch)
				if err != nil {
					return err
				}
				if c.Cfg != nil {
					p.admin.Namespaces().SetMaxConsumersPerSubscriptionWithContext(ctx, *name, c.Cfg.Subscription)
					p.admin.Namespaces().SetMaxConsumersPerTopicWithContext(ctx, *name, c.Cfg.Users)
					p.admin.Namespaces().SetMaxProducersPerTopicWithContext(ctx, *name, c.Cfg.Authors)
					p.admin.Namespaces().SetMaxTopicsPerNamespaceWithContext(ctx, *name, c.Cfg.PageSize)
				}
				return nil
			},
			retry.RetryIf(func(err error) bool {
				log.Printf("error condition: %v", err)
				return err != nil // true if to attempt another retry
			}),
			retry.Attempts(p.cfg.RetryAttempts),
			retry.Delay(p.cfg.Delay),
			retry.DelayType(p.cfg.DelayType),
		)
	}
	return nil
}

func (p *IPubDBAdmin) PushObject(ctx context.Context, c *ICreateObject) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c == nil {
		return fmt.Errorf("[empty struct]")
	}

	if p.DoesObjectExists(ctx, c.Class, c.Bucket, c.Branch, c.Object) {
		log.Printf("[object with name %s already exist]", c.Object)
		return nil
	}

	switch c.Class {
	case DISK:
		return p.createObject(ctx, "persistent", c)
	case RAM:
		return p.createObject(ctx, "non-persistent", c)
	default:
		return fmt.Errorf("[invalid object class %q]", c.Class)
	}
}

// DoesBucketExists true if topic tenant exists
func (p *IPubDBAdmin) DoesBucketExists(ctx context.Context, bucket string) bool {
	_, err := p.admin.Tenants().GetWithContext(ctx, bucket)
	return err == nil
}

// DoesBranchExists true if namesapce exists
func (p *IPubDBAdmin) DoesBranchExists(ctx context.Context, bucket, branch string) bool {
	url, err := utils.GetNameSpaceName(bucket, branch)
	if err != nil {
		log.Println(err)
		return false
	}
	_, err = p.admin.Namespaces().GetPoliciesWithContext(ctx, url.String())
	return err == nil
}

// DoesObjectExists true if topic exists
func (p *IPubDBAdmin) DoesObjectExists(ctx context.Context, c Class, bucket, branch, object string) bool {
	topic := createURL(c, bucket, branch, object)
	name, err := utils.GetTopicName(topic)
	if err != nil {
		return false
	}
	_, err = p.admin.Topics().GetMetadataWithContext(ctx, *name)
	return err == nil
}

// PullObjects returns disk:[]getobject or ram:[]object
func (p *IPubDBAdmin) PullObjects(ctx context.Context, bucket, branch string) map[Class][]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.DoesBucketExists(ctx, bucket) || !p.DoesBranchExists(ctx, bucket, branch) {
		return nil
	}

	url, err := utils.GetNameSpaceName(bucket, branch)
	if err != nil {
		log.Println(err)
		return nil
	}

	// persistent & non-persistent
	pr, np, err := p.admin.Topics().ListWithContext(ctx, *url)
	if err != nil {
		log.Println(err)
		return nil
	}
	data := make(map[Class][]string)
	data[DISK] = topicObjects(pr)
	data[RAM] = topicObjects(np)
	return data
}

func topicObjects(topics []string) []string {
	objects := make([]string, 0, len(topics))
	for _, topic := range topics {
		parts := strings.Split(strings.TrimSuffix(topic, "-partition-0"), "/")
		if len(parts) > 0 {
			objects = append(objects, parts[len(parts)-1])
		}
	}
	return objects
}

// PullBranches returns total number of branches
func (p *IPubDBAdmin) PullBranches(ctx context.Context, bucket string) []string {
	ns, err := p.admin.Namespaces().GetNamespacesWithContext(ctx, bucket)
	if err != nil {
		log.Println(err)
		return nil
	}
	branches := make([]string, 0, len(ns))
	prefix := bucket + "/"
	for _, namespace := range ns {
		branches = append(branches, strings.TrimPrefix(namespace, prefix))
	}
	return branches
}

// PullBuckets total number of tenants
func (p *IPubDBAdmin) PullBuckets(ctx context.Context) []string {
	bs, err := p.admin.Tenants().ListWithContext(ctx)
	if err != nil {
		log.Println(err)
	}
	return bs
}

// GetBranch returns the object releated to that namesapce
func (p *IPubDBAdmin) GetBranch(ctx context.Context, bucket, branch string) []string {
	url, err := utils.GetNameSpaceName(bucket, branch)
	if err != nil {
		log.Println(err)
		return nil
	}
	persistent, nonPersistent, err := p.admin.Topics().ListWithContext(ctx, *url)
	if err != nil {
		log.Println(err)
		return nil
	}
	return append(topicObjects(persistent), topicObjects(nonPersistent)...)
}

// ReadObjectStats returns the topic stats
func (p *IPubDBAdmin) ReadObjectStats(ctx context.Context, bucket, branch, object string, handle func(data utils.TopicStats)) error {
	url := createURL(RAM, bucket, branch, object)
	f, err := utils.GetTopicName(url)
	if err != nil {
		return err
	}
	ns, err := p.admin.Topics().GetStats(*f)
	if err != nil {
		return err
	}
	if handle != nil {
		handle(ns)
	}
	return nil
}

// IsObjectParition returns true if topic found to be partionied
func (p *IPubDBAdmin) IsObjectParition(ctx context.Context, bucket, branch, object string) (bool, error) {
	return p.isObjectPartition(ctx, RAM, bucket, branch, object)
}

func (p *IPubDBAdmin) isObjectPartition(ctx context.Context, class Class, bucket, branch, object string) (bool, error) {
	if !p.DoesBucketExists(ctx, bucket) || !p.DoesBranchExists(ctx, bucket, branch) {
		return false, fmt.Errorf("[nor bucket %s or branch found %s]", bucket, branch)
	}
	f, err := utils.GetTopicName(createURL(class, bucket, branch, object))
	if err != nil {
		return false, err
	}
	x, err := p.admin.Topics().GetPartitionedStats(*f, true)
	if err != nil {
		return false, err
	}
	return len(x.Partitions) > 0, err
}

// DeleteObject deletes the topic
func (p *IPubDBAdmin) DeleteObject(ctx context.Context, bucket, branch, object string) error {
	return p.deleteObject(ctx, RAM, bucket, branch, object)
}

func (p *IPubDBAdmin) deleteObject(ctx context.Context, class Class, bucket, branch, object string) error {
	if !p.DoesBucketExists(ctx, bucket) || !p.DoesBranchExists(ctx, bucket, branch) {
		return fmt.Errorf("[nor bucket %s or branch found %s]", bucket, branch)
	}
	f, err := utils.GetTopicName(createURL(class, bucket, branch, object))

	if err != nil {
		return err
	}
	ipat, err := p.isObjectPartition(ctx, class, bucket, branch, object)
	if err != nil {
		return err
	}
	return p.admin.Topics().Delete(*f, true, ipat)
}

// DeleteBranch deletes the namespace
func (p *IPubDBAdmin) DeleteBranch(ctx context.Context, bucket, branch string) error {
	if !p.DoesBucketExists(ctx, bucket) || !p.DoesBranchExists(ctx, bucket, branch) {
		return fmt.Errorf("[invalid call to delete]")
	}
	url, err := utils.GetNameSpaceName(bucket, branch)
	if err != nil {
		return err
	}
	return p.admin.Namespaces().DeleteNamespace(url.String())
}

// DeleteBucket delets the tenant
func (p *IPubDBAdmin) DeleteBucket(ctx context.Context, bucket string) error {
	return p.admin.Tenants().Delete(bucket)
}

// Release deletes all the data stored in the pulsar starting from buckets->branches->objects
func (p *IPubDBAdmin) Release(ctx context.Context) {
	for _, bucket := range p.PullBuckets(ctx) {
		if bucket == "public" || bucket == "pulsar" {
			continue
		}
		for _, branch := range p.PullBranches(ctx, bucket) {
			objects := p.PullObjects(ctx, bucket, branch)
			for class, names := range objects {
				for _, object := range names {
					if err := p.deleteObject(ctx, class, bucket, branch, object); err != nil {
						log.Println(err)
					}
				}
			}
			if err := p.DeleteBranch(ctx, bucket, branch); err != nil {
				log.Println(err)
			}
		}
		if err := p.DeleteBucket(ctx, bucket); err != nil {
			log.Println(err)
		}
	}
}

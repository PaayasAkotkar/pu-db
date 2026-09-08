package pudb

import (
	"context"
	"fmt"
	"pu/app/common"
	vadb "va/app/core"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

type IResult struct {
	Ready bool
	Pull  *vadb.IPull
}

type Handler func(data *IResult)

const (
	MaxPendingMessages = 120
	//WinBucket          = "windows"
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

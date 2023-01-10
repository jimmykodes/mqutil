package pubsub

import (
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/jimmykodes/gommand"
	"github.com/jimmykodes/gommand/flags"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	subscriptionCmd = &gommand.Command{
		Name:        "subs",
		Description: "subscription utilities",
		PersistentFlags: []flags.Flag{
			flags.StringFlagS("topic", 't', "", "PubSub Topic"),
		},
	}
	subscriptionSubCommands = []*gommand.Command{
		{
			Name:        "list",
			Usage:       "list",
			Description: "list subscriptions for a given topic",
			Run:         withClient(listSubscriptions),
		},
		{
			Name:         "create",
			Usage:        "create [subscriptions...]",
			Description:  "create subscriptions for a given topic",
			ArgValidator: gommand.ArgsMin(1),
			Run:          withClient(createSubscriptions),
		},
		{
			Name:         "delete",
			Usage:        "delete [subscriptions...]",
			Description:  "delete subscriptions for a given topic",
			ArgValidator: gommand.ArgsMin(1),
			Run:          withClient(deleteSubscriptions),
		},
	}
)

func createSubscriptions(ctx *gommand.Context, client *pubsub.Client) error {
	topicName, err := ctx.Flags().LookupString("topic")
	if err != nil {
		fmt.Println("error getting topic", err)
		return err
	}
	topic := client.Topic(topicName)

	for _, t := range ctx.Args() {
		subscription, err := client.CreateSubscription(ctx, t, pubsub.SubscriptionConfig{Topic: topic})
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			fmt.Println("subscription already exists")
			continue
		}
		if err != nil {
			fmt.Println("error creating subscription", t)
			return err
		}
		fmt.Println("created subscription:", subscription.String())
	}
	return nil
}

func listSubscriptions(ctx *gommand.Context, client *pubsub.Client) error {
	topicName, err := ctx.Flags().LookupString("topic")
	if err != nil {
		fmt.Println("error getting topic", err)
		return err
	}
	topic := client.Topic(topicName)

	subs := topic.Subscriptions(ctx)
	for {
		sub, err := subs.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Println("error listing subscriptions for topic", err)
			return err
		}
		fmt.Println(sub.String())
	}
	return nil
}

func deleteSubscriptions(ctx *gommand.Context, client *pubsub.Client) error {
	topicName, err := ctx.Flags().LookupString("topic")
	if err != nil {
		fmt.Println("error getting topic", err)
		return err
	}
	topic := client.Topic(topicName)

	subMap := make(map[string]struct{}, len(ctx.Args()))
	for _, subName := range ctx.Args() {
		subMap[subName] = struct{}{}
	}
	subs := topic.Subscriptions(ctx)
	for {
		sub, err := subs.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Println("error listing subscriptions for topic", err)
			return err
		}
		if _, ok := subMap[sub.ID()]; !ok {
			continue
		}
		delete(subMap, sub.ID())
		if err := sub.Delete(ctx); err != nil {
			fmt.Printf("error deleting subscriptions %s for topic %s: %v\n", sub.ID(), topic.ID(), err)
			return err
		}
		fmt.Println("deleted subscription:", sub.String())
	}
	return nil
}

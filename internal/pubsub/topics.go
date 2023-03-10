package pubsub

import (
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/jimmykodes/gommand"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	topicCmd = &gommand.Command{
		Name:        "topics",
		Description: "interact with pubsub topics",
	}
	topicSubCommands = []*gommand.Command{
		{
			Name:        "list",
			Description: "list topics",
			Run:         withClient(listTopics),
		},
		{
			Name:         "create",
			Description:  "create a pubsub topic",
			ArgValidator: gommand.ArgsMin(1),
			Run:          withClient(createTopics),
		},
		{
			Name:         "delete",
			Description:  "delete topics",
			ArgValidator: gommand.ArgsMin(1),
			Run:          withClient(deleteTopics),
		},
	}
)

func listTopics(ctx *gommand.Context, client *pubsub.Client) error {
	ti := client.Topics(ctx)
	for {
		t, err := ti.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Println("error getting topics", err)
			return err
		}
		fmt.Println(t.String())
	}
	return nil
}
func createTopics(ctx *gommand.Context, client *pubsub.Client) error {
	for _, t := range ctx.Args() {
		topicName, subscriptions, found := strings.Cut(t, ":")
		topic, err := client.CreateTopic(ctx, topicName)
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			fmt.Println("topic already exists")
			topic = client.Topic(topicName)
		} else if err != nil {
			fmt.Println("error creating topic", t)
			return err
		} else {
			fmt.Println("created topic", topic.String())
		}

		if found {
			for _, sub := range strings.Split(subscriptions, ";") {
				subscription, err := client.CreateSubscription(ctx, sub, pubsub.SubscriptionConfig{Topic: topic})
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
		}
	}
	return nil
}
func deleteTopics(ctx *gommand.Context, client *pubsub.Client) error {
	for _, topicName := range ctx.Args() {
		topic := client.Topic(topicName)
		subs := topic.Subscriptions(ctx)
		for {
			sub, err := subs.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				fmt.Println("error getting subscriptions", err)
				return err
			}
			if err := sub.Delete(ctx); err != nil {
				fmt.Printf("error deleting subscription %s for topic %s: %v\n", sub.ID(), topicName, err)
				return err
			}
		}
		if err := topic.Delete(ctx); err != nil {
			fmt.Printf("error deleting topic %s: %v\n", topicName, err)
		}
	}
	return nil
}

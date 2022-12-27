package pubsub

import (
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/jimmykodes/gommand"
	"github.com/jimmykodes/gommand/flags"
)

func Command() (*gommand.Command, error) {
	rootCmd := &gommand.Command{
		Name: "pubsub",
		PersistentFlags: []flags.Flag{
			flags.StringFlagS("project", 'p', "", "GCP Project"),
			flags.StringFlagS("host", 'H', "", "PubSub Emulator Host to use instead of production PubSub"),
		},
		Description: "utilities for interacting with Google PubSub",
		PersistentPreRun: func(ctx *gommand.Context) error {
			if host := ctx.Flags().String("host"); host != "" {
				if err := os.Setenv("PUBSUB_EMULATOR_HOST", host); err != nil {
					fmt.Println("error setting pubsub emulator host:", err)
					return err
				}
			}
			return nil
		},
	}

	rootCmd.SubCommand(subscriptionCmd, topicCmd, produceCmd, subscribeCmd)

	subscriptionCmd.SubCommand(subscriptionSubCommands...)
	topicCmd.SubCommand(topicSubCommands...)
	produceCmd.SubCommand(produceSubCommands...)

	if err := rootCmd.PersistentFlagSet().MarkRequired("project"); err != nil {
		return nil, err
	}
	return rootCmd, nil
}

func withClient(f func(*gommand.Context, *pubsub.Client) error) func(ctx *gommand.Context) error {
	return func(ctx *gommand.Context) error {
		project, err := ctx.Flags().LookupString("project")
		if err != nil {
			return err
		}
		client, err := pubsub.NewClient(ctx, project)
		if err != nil {
			fmt.Println("error creating pubsub client:", err)
			return err
		}
		defer client.Close()
		return f(ctx, client)
	}
}

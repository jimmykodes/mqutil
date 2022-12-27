package pubsub

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/jimmykodes/gommand"
)

var (
	subscribeCmd = &gommand.Command{
		Name:         "subscribe",
		Usage:        "subscribe subscription-name",
		Description:  "subscribe to events from this subscription",
		ArgValidator: gommand.ArgsExact(1),
		Run: withClient(func(ctx *gommand.Context, client *pubsub.Client) error {
			sigCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			err := client.Subscription(ctx.Args()[0]).Receive(sigCtx, func(ctx context.Context, m *pubsub.Message) {
				defer m.Ack()

				fmt.Println("message received", m.ID)
				fmt.Println(string(m.Data))
			})
			if err != nil {
				fmt.Println("error receiving messages:", err)
				return err
			}

			return nil
		}),
	}
)

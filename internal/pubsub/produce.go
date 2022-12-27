package pubsub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jimmykodes/gommand"
	"github.com/jimmykodes/gommand/flags"
)

var (
	produceCmd = &gommand.Command{
		Name:        "produce",
		Description: "produce messages to a topic",
		PersistentFlags: []flags.Flag{
			flags.StringFlagS("topic", 't', "local-test", "pubsub topic to write to"),
		},
	}
	produceSubCommands = []*gommand.Command{
		{
			Name:        "one",
			Description: "send a single message to the topic",
			Flags: []flags.Flag{
				flags.StringFlagS("file", 'f', "", "file to send as message data (default to stdin)"),
			},
			Run: withClient(publishOne),
		},
		{
			Name:        "svr",
			Description: "run an http server that forwards request bodies as pubsub message data",
			Flags: []flags.Flag{
				flags.IntFlag("port", 8080, "port to run http server on"),
			},
			Run: withClient(publishSvr),
		},
	}
)

func publishOne(ctx *gommand.Context, client *pubsub.Client) error {
	var topic = client.Topic(ctx.Flags().String("topic"))
	defer topic.Flush()

	var (
		src  io.ReadCloser
		file = ctx.Flags().String("file")
	)
	if file == "" {
		src = os.Stdin
	} else {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("error opening file", err)
			return fmt.Errorf("invalid file")
		}
		src = f
	}
	defer src.Close()

	var data bytes.Buffer
	if _, err := io.Copy(&data, src); err != nil {
		fmt.Println("error reading data", err)
		return err
	}

	id, err := topic.Publish(ctx, &pubsub.Message{Data: data.Bytes()}).Get(ctx)
	if err != nil {
		fmt.Println("error publishing message", err)
		return err
	}

	fmt.Println("published message:", id)
	return nil

}
func publishSvr(ctx *gommand.Context, client *pubsub.Client) error {
	topic := client.Topic(ctx.Flags().String("topic"))
	defer topic.Flush()

	sigCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var data bytes.Buffer
		_, err := io.Copy(&data, r.Body)
		if err != nil {
			log.Println("error reading body", err)
			http.Error(w, "error reading body", http.StatusInternalServerError)
			return
		}
		id, err := topic.Publish(r.Context(), &pubsub.Message{Data: data.Bytes()}).Get(r.Context())
		if err != nil {
			log.Println("error sending message", err)
			http.Error(w, "error sending message", http.StatusInternalServerError)
			return
		}
		log.Println("sent message", id)
		w.WriteHeader(http.StatusCreated)
		return
	})

	svr := http.Server{
		Addr:    fmt.Sprintf(":%d", ctx.Flags().Int("port")),
		Handler: mux,
	}

	go func() {
		<-sigCtx.Done()
		svr.SetKeepAlivesEnabled(false)
		toCtx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if err := svr.Shutdown(toCtx); err != nil {
			fmt.Println("error shutting down server:", err)
		}
	}()

	fmt.Println("running producer server at", svr.Addr)
	if err := svr.ListenAndServe(); err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		fmt.Println("server error:", err)
		return err
	}
	return nil
}

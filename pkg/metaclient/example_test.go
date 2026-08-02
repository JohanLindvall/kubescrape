package metaclient_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

func Example() {
	c := metaclient.New(metaclient.Config{
		Base:    "http://kubescrape.monitoring",
		Timeout: 15 * time.Second,
		// Optional: feed lookup outcomes into your own metrics. The package
		// itself has no metrics dependency.
		Observe: func(outcome string) { fmt.Println("lookup:", outcome) },
	})

	// A container ID may reach the agent before the kubelet has posted it to
	// the API server; the service holds the request up to the wait.
	md, err := c.Container(context.Background(), "containerd://0123abc...", 2*time.Second)
	if metaclient.IsNotFound(err) {
		fmt.Println("unknown container")
		return
	}
	if err == nil {
		fmt.Println(md.Pod.Namespace, md.Pod.Name)
	}
}

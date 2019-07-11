package inmemory

import (
	"encoding/json"
	"fmt"
)

type JSONNotifier struct {
	// Channel of notifications
	notifications chan []byte
	// channel of clients wanting to subscribe
	joins chan chan []byte
	// channel of clients closing/closing
	exits chan chan []byte
	// map of client channels.
	clients map[chan []byte]bool
	// Channel to stop operations
	stop chan bool
}

func (n *JSONNotifier) Notify(data interface{}) error {
	marshaled, err := json.Marshal(data)
	if err != nil {
		return err
	}
	n.notifications <- marshaled
	return nil
}

func (n *JSONNotifier) Subscribe(client chan []byte) chan interface{} {
	done := make(chan interface{})
	n.joins <- client

	go func() {
		<-done
		n.exits <- client
	}()

	return done
}

func (n *JSONNotifier) Run() {
	fmt.Println("Launching notifier.")
	for {
		select {
		case client := <-n.joins:
			n.clients[client] = true
		case client := <-n.exits:
			delete(n.clients, client)
		case event := <-n.notifications:
			// Nonblocking sends; if a client isn't ready,
			// it'll just miss the event.  Too bad.
			for client, _ := range n.clients {
				select {
				case client <- event:
					continue
				default:
					continue
				}
			}
		case <-n.stop:
			break
		}
	}
	fmt.Println("Stopping notifier")
	close(n.notifications)
	close(n.exits)
	close(n.joins)
	close(n.stop)
}

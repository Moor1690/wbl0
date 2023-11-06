package main

import (
	"log"

	"github.com/nats-io/stan.go"
)

func natsSubscription(dataCh chan string) {
	nc, err := stan.Connect(clusterID, clientID)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	_, err = nc.Subscribe(subject, func(msg *stan.Msg) {
		dataCh <- string(msg.Data)
	}, stan.DeliverAllAvailable())
	if err != nil {
		log.Fatal(err)
	}

	select {}
}

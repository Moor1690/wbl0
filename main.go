package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

const clusterID = "test-cluster"
const clientID = "client-id"
const subject = "test-subject"

func main() {

	dataCh := make(chan string)
	ctx := context.Background()
	var orderMap map[string]Order = make(map[string]Order)

	config, err := pgxpool.ParseConfig("postgres://user:0000@localhost:5432/wb")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse connection string: %v\n", err)
		os.Exit(1)
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	go GetAllOrders(ctx, pool, orderMap)

	go natsSubscription(dataCh)

	go InsertOrder(ctx, pool, dataCh, orderMap)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		inputPageHandler(w, r)
	})

	http.HandleFunc("/show", func(w http.ResponseWriter, r *http.Request) {
		showPageHandler(w, r, orderMap)
	})

	http.HandleFunc("/notfound", func(w http.ResponseWriter, r *http.Request) {
		notFoundPageHandler(w, r)
	})

	http.ListenAndServe(":8000", nil)
}

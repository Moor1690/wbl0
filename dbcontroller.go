package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

func InsertOrder(ctx context.Context, conn *pgxpool.Pool, dataCh chan string, orders map[string]Order) {

	for jsonData := range dataCh {
		var o Order
		err := json.Unmarshal([]byte(jsonData), &o)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}

		deliveryJSON, err := json.Marshal(o.Delivery)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}
		paymentJSON, err := json.Marshal(o.Payment)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}
		itemsJSON, err := json.Marshal(o.Items)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}

		if err := o.Validate(); err != nil {
			fmt.Println("error:", err)
			continue
		}

		if val, exists := orders[o.OrderUID]; exists {
			fmt.Println("order with this uid already exists", val.OrderUID)
			continue
		}

		orders[o.OrderUID] = o

		sql := `INSERT INTO orders (order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err = conn.Exec(ctx, sql, o.OrderUID, o.TrackNumber, o.Entry, deliveryJSON, paymentJSON, itemsJSON, o.Locale, o.InternalSignature, o.CustomerID, o.DeliveryService, o.ShardKey, o.SmID, o.DateCreated, o.OofShard)

		if err != nil {
			fmt.Println("Error inserting order:", err)
			continue
		}
	}
}

func GetAllOrders(ctx context.Context, pool *pgxpool.Pool, orders map[string]Order) error {

	rows, err := pool.Query(ctx, `
        SELECT 
            order_uid, track_number, entry, 
            delivery, payment, items, locale, 
            internal_signature, customer_id, 
            delivery_service, shardkey, sm_id, 
            date_created, oof_shard
        FROM orders
    `)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var order Order
		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry,
			&order.Delivery, &order.Payment, &order.Items, &order.Locale,
			&order.InternalSignature, &order.CustomerID,
			&order.DeliveryService, &order.ShardKey, &order.SmID,
			&order.DateCreated, &order.OofShard,
		)
		if err != nil {
			return err
		}
		orders[order.OrderUID] = order
	}

	return nil
}

package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func InsertOrder(ctx context.Context, conn *pgx.Conn, o Order) error {
	deliveryJSON, err := json.Marshal(o.Delivery)
	if err != nil {
		return err
	}
	paymentJSON, err := json.Marshal(o.Payment)
	if err != nil {
		return err
	}
	itemsJSON, err := json.Marshal(o.Items)
	if err != nil {
		return err
	}

	sql := `INSERT INTO orders (order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

	_, err = conn.Exec(ctx, sql, o.OrderUID, o.TrackNumber, o.Entry, deliveryJSON, paymentJSON, itemsJSON, o.Locale, o.InternalSignature, o.CustomerID, o.DeliveryService, o.ShardKey, o.SmID, o.DateCreated, o.OofShard)
	return err
}

// func GetAllOrders(ctx context.Context, pool *pgxpool.Pool) ([]Order, error) {
// 	orders := []Order{}
// 	rows, err := pool.Query(ctx, `
//         SELECT
//             order_uid, track_number, entry,
//             delivery, payment, items, locale,
//             internal_signature, customer_id,
//             delivery_service, shardkey, sm_id,
//             date_created, oof_shard
//         FROM orders
//     `)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var order Order
// 		err := rows.Scan(
// 			&order.OrderUID, &order.TrackNumber, &order.Entry,
// 			&order.Delivery, &order.Payment, &order.Items, &order.Locale,
// 			&order.InternalSignature, &order.CustomerID,
// 			&order.DeliveryService, &order.ShardKey, &order.SmID,
// 			&order.DateCreated, &order.OofShard,
// 		)
// 		if err != nil {
// 			return nil, err
// 		}
// 		orders = append(orders, order)
// 	}
// 	return orders, nil
// }

func GetAllOrders(ctx context.Context, pool *pgxpool.Pool) (map[string]Order, error) {
	orders := make(map[string]Order)

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
		return nil, err
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
			return nil, err
		}
		orders[order.OrderUID] = order
	}

	return orders, nil
}

func insertToDB(jsonData *string) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://user:0000@localhost:5432/wb")
	if err != nil {
		fmt.Println("Unable to connect to database:", err)
		return
	}
	defer conn.Close(ctx)

	var newOrder Order
	err3 := json.Unmarshal([]byte(*jsonData), &newOrder)
	if err3 != nil {
		fmt.Println("error:", err3)
		return
	}

	if err := InsertOrder(ctx, conn, newOrder); err != nil {
		fmt.Println("Failed to insert order:", err)
	}
}

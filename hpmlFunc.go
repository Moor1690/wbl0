package main

import (
	"net/http"
	"strconv"
)

func inputPageHandler(w http.ResponseWriter, r *http.Request) {
	html := `
	<!DOCTYPE html>
	<html>
	<head>
		<title>Enter OrderUID</title>
	</head>
	<body>
		<h2>Enter OrderUID</h2>
		<form action="/show" method="post">
			<label for="OrderUID">OrderUID:</label><br>
			<input type="text" id="OrderUID" name="OrderUID" required><br><br>
			<input type="submit" value="Submit">
		</form>
	</body>
	</html>
	`

	w.Write([]byte(html))
}

func showPageHandler(w http.ResponseWriter, r *http.Request, orderData map[string]Order) {
	if r.Method == http.MethodPost {
		orderUID := r.FormValue("OrderUID")

		o, exists := orderData[orderUID]

		if exists {
			html := `
			<!DOCTYPE html>
			<html lang="en">
			<head>
				<meta charset="UTF-8">
				<meta name="viewport" content="width=device-width, initial-scale=1.0">
				<title>Order Details</title>
			</head>
			<body>
				<h2>Order Details</h2>
				<table border="1">
					<tr><td>Order UID</td><td>` + o.OrderUID + `</td></tr>
					<tr><td>Track Number</td><td>` + o.TrackNumber + `</td></tr>
					<tr><td>Entry</td><td>` + o.Entry + `</td></tr>
					<tr><td>Locale</td><td>` + o.Locale + `</td></tr>
					<tr><td>Internal Signature</td><td>` + o.InternalSignature + `</td></tr>
					<tr><td>Customer ID</td><td>` + o.CustomerID + `</td></tr>
					<tr><td>Delivery Service</td><td>` + o.DeliveryService + `</td></tr>
					<tr><td>Shard Key</td><td>` + o.ShardKey + `</td></tr>
					<tr><td>Sm ID</td><td>` + strconv.Itoa(o.SmID) + `</td></tr>
					<tr><td>Date Created</td><td>` + o.DateCreated.String() + `</td></tr>
					<tr><td>Oof Shard</td><td>` + o.OofShard + `</td></tr>
			
					<tr><td colspan="2">Delivery</td></tr>
					<tr><td>Name</td><td>` + o.Delivery.Name + `</td></tr>
					<tr><td>Phone</td><td>` + o.Delivery.Phone + `</td></tr>
					<tr><td>Zip</td><td>` + o.Delivery.Zip + `</td></tr>
					<tr><td>City</td><td>` + o.Delivery.City + `</td></tr>
					<tr><td>Address</td><td>` + o.Delivery.Address + `</td></tr>
					<tr><td>Region</td><td>` + o.Delivery.Region + `</td></tr>
					<tr><td>Email</td><td>` + o.Delivery.Email + `</td></tr>
			
					<tr><td colspan="2">Payment</td></tr>
					<tr><td>Transaction</td><td>` + o.Payment.Transaction + `</td></tr>
					<tr><td>Request ID</td><td>` + o.Payment.Request_id + `</td></tr>
					<tr><td>Currency</td><td>` + o.Payment.Currency + `</td></tr>
					<tr><td>Provider</td><td>` + o.Payment.Provider + `</td></tr>
					<tr><td>Amount</td><td>` + strconv.Itoa(o.Payment.Amount) + `</td></tr>
					<tr><td>Payment Date</td><td>` + strconv.Itoa(o.Payment.Payment_dt) + `</td></tr>
					<tr><td>Bank</td><td>` + o.Payment.Bank + `</td></tr>
					<tr><td>Delivery Cost</td><td>` + strconv.Itoa(o.Payment.Delivery_cost) + `</td></tr>
					<tr><td>Goods Total</td><td>` + strconv.Itoa(o.Payment.Goods_total) + `</td></tr>
					<tr><td>Custom Fee</td><td>` + strconv.Itoa(o.Payment.Custom_fee) + `</td></tr>
			
					<tr><td colspan="2">Items</td></tr>
				`
			for _, item := range o.Items {
				html += `
						<tr><td>Chrt ID</td><td>` + strconv.Itoa(item.Chrt_id) + `</td></tr>
						<tr><td>Track Number</td><td>` + item.Track_number + `</td></tr>
						<tr><td>Price</td><td>` + strconv.Itoa(item.Price) + `</td></tr>
						<tr><td>Rid</td><td>` + item.Rid + `</td></tr>
						<tr><td>Name</td><td>` + item.Name + `</td></tr>
						<tr><td>Sale</td><td>` + strconv.Itoa(item.Sale) + `</td></tr>
						<tr><td>Size</td><td>` + item.Size + `</td></tr>
						<tr><td>Total Price</td><td>` + strconv.Itoa(item.Total_price) + `</td></tr>
						<tr><td>Nm ID</td><td>` + strconv.Itoa(item.Nm_id) + `</td></tr>
						<tr><td>Brand</td><td>` + item.Brand + `</td></tr>
						<tr><td>Status</td><td>` + strconv.Itoa(item.Status) + `</td></tr>
					`
			}
			html += `
    </table>
</body>
</html>
`

			w.Write([]byte(html))
		} else {
			http.Redirect(w, r, "/notfound", http.StatusSeeOther)
		}
	} else {
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

func notFoundPageHandler(w http.ResponseWriter, r *http.Request) {
	html := `
	<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>404 Not Found</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 40px;
            text-align: center;
        }
        .container {
            display: inline-block;
            padding: 20px;
            border: 1px solid #ccc;
            box-shadow: 2px 2px 12px #aaa;
        }
    </style>
</head>
<body>
    <h1>404 - Order Not Found</h1>
    <p>We couldn't find the order you were looking for.</p>
    </body>
</html>

	`
	w.Write([]byte(html))
}

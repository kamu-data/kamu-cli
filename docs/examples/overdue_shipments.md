# Example: Overdue Order Shipments Detection

- [Example: Overdue Order Shipments Detection](#example-overdue-order-shipments-detection)
- [Summary](#summary)
- [Steps](#steps)
  - [Getting Started](#getting-started)
  - [Root Datasets](#root-datasets)
  - [Joining Shipments with Orders](#joining-shipments-with-orders)
  - [Computing total quantity of shipped product](#computing-total-quantity-of-shipped-product)
  - [Detecting late shipments](#detecting-late-shipments)
  - [Reminder about Watermarks](#reminder-about-watermarks)

**Topics covered:**
- Stream-to-Stream joins
- Temporal aggregations
- Watermarks

# Summary
In this example we will take a look at an imaginary e-commerce company `acme.com` committed to shipping their products to customers as soon as possible. To maintain their level of service they have decided to create an alerting system using `kamu` that would notify them if any order did not ship within a certain time window, so that they could investigate the root cause of the delay.

# Steps

## Getting Started
To follow this example checkout `kamu-cli` repository and navigate into `examples/overdue_shipments` sub-directory.

Create a temporary kamu workspace in that folder using:

```sh
$ kamu init
```

You can either follow the example steps below or fast-track through it by running:

```sh
# Add all dataset manifests found in the current directory
$ kamu add . --recursive
$ kamu pull --all
```

## Root Datasets
We will be using two root datasets:
- `com.acme.orders` - contains a log of orders as they are being placed in the system. To make it more interesting we will assume that each order can request different quantity of "the product" that can be shipped to the customer in one or several shipments.
- `com.acme.shipments` - contains a log of shipments as they are being dispatched to the customers. Every shipment links to the original order and specifies the quantity of "the product" that has been shipped.

Both datasets are sourcing their data from files located in `data/` sub-directory. Once comfortable with this example you can try adding more data in `orders-xxx.csv` and `shipments-xxx.csv` to see how `fileGlob` data sources work and test the queries with any edge cases you can come up with.

Let's add the root datasets and ingest the data:

```sh
$ kamu add com.acme.orders.yaml com.acme.shipments.yaml
$ kamu pull --all
```

You can verify the result using the SQL shell:

```sql
$ kamu sql
0: kamu> select * from `com.acme.orders`;
+-------------+------------+-----------+-----------+
| system_time | event_time | order_id  | quantity  |
+-------------+------------+-----------+-----------+
|     ...     | 2020-01-01 | 1         | 10        |
|     ...     | 2020-01-01 | 2         | 120       |
|     ...     | 2020-01-05 | 3         | 9         |
|     ...     | 2020-01-10 | 4         | 110       |
+-------------+------------+-----------+-----------+
4 rows selected (0.11 seconds)

0: kamu> select * from `com.acme.shipments`;
+-------------+------------+--------------+-----------+-----------+
| system_time | event_time | shipment_id  | order_id  | quantity  |
+-------------+------------+--------------+-----------+-----------+
|     ...     | 2020-01-01 | 1            | 1         | 4         |
|     ...     | 2020-01-02 | 3            | 2         | 120       |
|     ...     | 2020-01-02 | 2            | 1         | 6         |
|     ...     | 2020-01-08 | 4            | 3         | 9         |
|     ...     | 2020-01-11 | 5            | 4         | 50        |
|     ...     | 2020-01-13 | 6            | 4         | 60        |
+-------------+------------+--------------+-----------+-----------+
```

## Joining Shipments with Orders
The goal of the `com.acme.shipments.overdue` dataset that we will be creating is to detect orders that have not been fulfilled at all or were fulfilled only partially within a specified time interval - let's say `2 DAYS`. We want the alert to go off as soon such condition occurs - there's no use investigating the root cause of the late order when few weeks have already passed since then.

We can define our goal as: an order placed in the system is considered "late" if during the interval of `2 DAYS` after the order has been placed the total quantity of product shipped doesn't equal the quantity that has been ordered.

First we need to get these two quantities somehow in one dataset, so let's join the orders and shipments data using `order_id` as the key:

```sql
-- Using Flink SQL dialect
SELECT
  o.event_time as order_time,
  o.order_id,
  o.quantity as order_quantity,
  CAST(s.event_time as TIMESTAMP) as shipped_time,
  COALESCE(s.quantity, 0) as shipped_quantity
FROM `com.acme.orders` as o
LEFT JOIN `com.acme.shipments` as s
ON
  o.order_id = s.order_id
  AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
```

This is a Stream-to-Stream join which uses two join conditions.

Firstly, we match orders and shipments on `order_id` - this is pretty standard and something you would do in a regular SQL with non-temporal data. If we only have this condition the query would already work, but since `kamu` doesn't know how far apart in time the orders and shipments can be it will expect that order `1` can match a shipment that happens a century or even a billion years from now, so it will have to keep track of all the orders forever.

To prevent that we add another condition that restricts the `shipment.event_time` to a time interval `[order.event_time, order.event_time + 2 DAYS)`. This tells `kamu` that all orders that are older than two days can be forgotten since they will never match this condition.

Also note that we are especially interested in orders that had no shipments at all, so we are using `LEFT JOIN` that will produce a record even if nothing on the right side has matched our criteria. In this case all `s.*` fields will be `NULL`, so we are using `COALESCE` to replace `NULL` values with a zero when computing `shipped_quantity`.

Also note that we need to `CAST` the `s.event_time` to `TIMESTAMP`. This is a specific of Flink SQL which treats event time of stream as a special type. Every stream is allowed to have only one event time so when two streams are joined we need to cast one of the times to a plain `TIMESTAMP` type.

## Computing total quantity of shipped product
The query above will produce a new event for every shipment. Since one order can have multiple shipment events and we're only interested in total quantity shipped - we need to aggregate them.

```sql
SELECT
  TUMBLE_START(order_time, INTERVAL '1' DAY) as order_time,
  order_id,
  count(*) as num_shipments,
  min(shipped_time) as first_shipment,
  max(shipped_time) as last_shipment,
  max(order_quantity) as order_quantity,
  sum(shipped_quantity) as shipped_quantity_total
FROM order_shipments
GROUP BY TUMBLE(order_time, INTERVAL '1' DAY), order_id
```

Since in our previous query (which we aliased as `order_shipments`) we emit results using `order.event_time` as the event time - all records for the same order will have the same event time. Even when event times match exactly Flink requires us to use some form of windowing in aggregations so we use a 1-day interval and a tumbling window. As long as our interval is less than the time we chose for considering the shipment "late" - we will not delay the emission of the results.

## Detecting late shipments
The final step is very straightforward - we want to emit only records for orders that were not fully fulfilled:

```sql
SELECT *
FROM order_shipments_agg
WHERE order_quantity <> shipped_quantity_total
```

Putting all of this together, our final dataset will look like this:

```yaml
apiVersion: 1
kind: DatasetSnapshot
content:
  id: com.acme.shipments.overdue
  source:
    kind: derivative
    inputs:
    - com.acme.orders
    - com.acme.shipments
    transform:
      kind: sql
      engine: flink
      queries:
      - alias: order_shipments
        query: >
          SELECT
            o.event_time as order_time,
            o.order_id,
            o.quantity as order_quantity,
            CAST(s.event_time as TIMESTAMP) as shipped_time,
            COALESCE(s.quantity, 0) as shipped_quantity
          FROM `com.acme.orders` as o
          LEFT JOIN `com.acme.shipments` as s
          ON
            o.order_id = s.order_id
            AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
      - alias: order_shipments_agg
        query: >
          SELECT
            TUMBLE_START(order_time, INTERVAL '1' DAY) as order_time,
            order_id,
            count(*) as num_shipments,
            min(shipped_time) as first_shipment,
            max(shipped_time) as last_shipment,
            max(order_quantity) as order_quantity,
            sum(shipped_quantity) as shipped_quantity_total
          FROM order_shipments
          GROUP BY TUMBLE(order_time, INTERVAL '1' DAY), order_id
      - alias: com.acme.shipments.overdue
        query: >
          SELECT *
          FROM order_shipments_agg
          WHERE order_quantity <> shipped_quantity_total
  vocab:
    eventTimeColumn: order_time
```

Add this dataset and let's compute the result:

```sh
kamu add com.acme.shipments.overdue.yaml
kamu pull com.acme.shipments.overdue
```

## Reminder about Watermarks
If you look at the resulting data you will notice that we didn't get the alert for `order_id == 4` which shipped in two batches, last of which was late by one day. This is because the watermark of the `orders` dataset did not advance far enough to cover that period. Try setting the watermark manually and then running the processing again:

```sh
kamu pull com.acme.orders --set-watermark 2020-01-13T00:00:00Z
kamu pull --all
```

For a more detailed explanation of watermarks please see [this example](./trading.md).

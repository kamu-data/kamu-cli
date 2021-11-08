# Examples <!-- omit in toc -->

| Name                                                        |    Level     |                          Topics                           |
| ----------------------------------------------------------- | :----------: | :-------------------------------------------------------: |
| [Self-serve Demo](/images/demo/README.md)                   |   Beginner   |         datasets, repositories, data verification         |
| [Currency Conversion](./currency_conversion.md)             |   Beginner   |                   temporal-table joins                    |
| [Stock Market Trading Data Analysis](./trading.md)          |   Beginner   | aggregations, temporal-table joins, watermarks, notebooks |
| [COVID-19 Daily Cases](./covid19.md)                        |   Beginner   |      harmonization, unions, aggregations, notebooks       |
| [Housing Prices Analysis](./housing_prices.md)              | Intermediate |       GIS data, GIS functions, GIS joins, notebooks       |
| [Overdue Order Shipments Detection](./overdue_shipments.md) | Intermediate |            stream-to-stream joins, watermarks             |

To work with examples we recommend you to clone this repo and use directories in `examples/` as your workspaces. 

For instance:

```bash
git clone https://github.com/kamu-data/kamu-cli.git
cd kamu-cli/examples/currency_conversion

kamu init
kamu add . -r
kamu pull --all
```

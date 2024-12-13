from pyarrow import flight

query = "select * from 'kamu/co.alphavantage.tickers.daily.spy' limit 10"

# Connect to the endpoint
client = flight.connect(
    "grpc+tls://node.demo.kamu.dev:50050"
    # "grpc://localhost:50050"
)

# Authenticate with your app's API key
token_pair = client.authenticate_basic_token('kamu', 'kamu')
options = flight.FlightCallOptions(headers=[token_pair])

# Query and fetch a reader for the results
descriptor = flight.FlightDescriptor.for_command(query)
flight_info = client.get_flight_info(descriptor, options)
reader = client.do_get(flight_info.endpoints[0].ticket, options)

# Convert to a Pandas DataFrame and print the results
print(reader.read_pandas())

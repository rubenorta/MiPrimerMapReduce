import mincemeat

client = mincemeat.Client()
client.password = "ruben"
client.conn("localhost", mincemeat.DEFAULT_PORT)

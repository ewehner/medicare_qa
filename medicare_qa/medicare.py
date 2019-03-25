import logging
import os
import json
import csv
import signal
import requests
import asyncio
import aiohttp

# loop = asyncio.get_event_loop()
session = aiohttp.ClientSession()


async def post_data_to_server(json_row, session):

    async with session.post("https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures",
                     data=json_row) as resp:
        return await resp.json()

# def signal_handler(signal, frame):
#     loop.stop()
#     session.close()
#     sys.exit(0)
#
#
# signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":

    logging.info('Reading schema files')

    for root, dirs, files in os.walk('schemas/'):
        for file in files:

            # Open each schema file and read it to a list
            with open('schemas/' + file) as csv_file:
                cvs_reader = csv.reader(csv_file, delimiter=',')
                schema = [row for row in cvs_reader]

            # specify the correct corresponding data file by name
            data_filename = file[0:-3] + 'txt'

            # Open the corresponding datafile and read its data
            with open('data/' + data_filename) as data_file:
                data = [line.replace('\n', '') for line in data_file.readlines()]

            # Create a new data_row dict and build up what will become the json object
            # for each row of data
            data_row = {}
            for row in data:
                for scheme in schema:
                    # defining these for better readability
                    data_key = scheme[0]
                    data_length = int(scheme[1])
                    data_info = row[0:data_length]
                    data_type = scheme[2]

                    if data_type.rstrip() == 'BOOLEAN':
                        data_row[data_key] = 'true' if data_info == 1 else 'false'
                    elif data_type.rstrip() == 'INTEGER':
                        data_row[data_key] = int(row[0:data_length])
                    else:
                        data_row[data_key] = row[0:data_length]

                    row = row[data_length:]

                data_row_json = json.dumps(data_row)

                # For each row, post the data to the server

                loop = asyncio.get_event_loop()
                loop.run_until_complete(post_data_to_server(data_row_json, session))

                loop.run_until_complete(asyncio.sleep(100))
                loop.close()


import logging
import os
import json
import csv
import random
import asyncio
import aiohttp

API_URL = "https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures"


async def post_data_to_server(json_row):
    async with aiohttp.ClientSession() as session:
        async with session.post(API_URL, data=json_row) as resp:
            return await resp.json(), resp.status


if __name__ == "__main__":

    for root, dirs, files in os.walk('schemas/'):
        for file in files:
            print('Reading schema file ' + file)
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
            print('Reading data files ' + data_filename)
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

                # Repeat as necessary until max attempts are reached
                attempts = 1
                while True:
                    resp_json, status = loop.run_until_complete(post_data_to_server(data_row_json))
                    if random.choice(range(2)) == 1:
                        status = 400
                    print(status)
                    # Check that both the request status code is good
                    # and that the response data matches the data we sent
                    if status == 201 and resp_json == data_row:
                        logging.info('POST request successful for measure_id: {}'.format(data_row['measure_id']))
                        break
                    elif attempts > 5:
                        # If the request is failing after 5 tries, raise an exception
                        # and log the error. Also, would want to update metrics here, etc.
                        if status is not 201:
                            logging.error('POST request has failed too many times for measure_id: {}'.format(data_row['measure_id']))
                            raise Exception('POST request has failed too many times for measure_id: {}'.format(data_row['measure_id']))
                        elif resp_json != data_row:
                            logging.error('Data returned from server does not match for measure_id: {}'.format(data_row['measure_id']))
                            raise Exception('Data returned from server does not match for measure_id: {}'.format(data_row['measure_id']))
                        break
                    else:
                        attempts += 1
                        logging.warning('Retrying request for measure_id {} - Attempt number {}'.format(data_row['measure_id'].strip(), attempts))

                        # choose a random back off interval so that if the server
                        # is down, requests don't all pile up at the same time
                        backoff_interval = random.random()
                        loop.run_until_complete(asyncio.sleep(backoff_interval))

                        continue

            # If it made it this far without raising an exception, all went well
            print('Data transfer for ' + data_filename + ' was successful')
            logging.info('Data transfer for ' + data_filename + ' was successful.')

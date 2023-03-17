import logging
import azure.functions as func
import xml.etree.ElementTree as ET
import urllib.request

from azure.data.tables import TableServiceClient

def main(mytimer: func.TimerRequest) -> None:
    connection_string = "HIDDEN FOR SECURITY REASONS"
    table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = table_service_client.get_table_client(table_name="ExchangeRates")

    # Pulling latest rates from ECB website
    file = urllib.request.urlopen('https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml')
    data = file.read()
    file.close()
    root = ET.fromstring(data.decode("utf-8"))
    time = root[2][0].attrib['time']

    # Checking if latest rates are already in the table
    filterForLatest = "PartitionKey eq '" + time + "'"
    rateEntities = table_client.query_entities(filterForLatest)
    entityCount = 0
    areRatesUpdated = False
    for entity in rateEntities:
        entityCount += 1
        if entityCount >= 1:
            areRatesUpdated = True
            break
    
    if areRatesUpdated:
        logging.info("Exchange rates are already updated for " + time)
    else:
        logging.info("Exchange rates are not updated for " + time + ", updating database...")
        data = [{u'PartitionKey': time, u'RowKey': i.attrib['currency'],u'Rate': i.attrib['rate'], u'Date': time} for i in root[2][0]]
        for row in data:
            table_client.create_entity(entity=row)

            
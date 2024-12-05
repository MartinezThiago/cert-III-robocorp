"""Imports modules"""

from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables
from robocorp import workitems
json = JSON()
http = HTTP()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic.json"

# JSON data keys
COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"

@task
def produce_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Produces traffic data work items.
    """
    print("produce")
    http.download(
        url="https://raw.githubusercontent.com/robocorp/inhuman-insurance-inc/main/RS_198.json",
        target_file=TRAFFIC_JSON_FILE_PATH,
        overwrite="True",
    )
    traffic_data = load_traffic_data_as_table()
    # table.write_table_to_csv(traffic_data, "output/traffic_data.csv")
    filtered_data = filter_and_sort_traffic_data(traffic_data)
    # table.write_table_to_csv(filtered_data, "output/filtered_data.csv")
    filtered_data = get_latest_data_by_country(filtered_data)
    payloads = create_work_item_payload(filtered_data)
    save_work_item_payloads(payloads)


@task
def consume_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Consumes traffic data work items.
    """
    print("consume")


def load_traffic_data_as_table():
    """Parse traffic data into a table"""
    json_data = json.load_json_from_file(TRAFFIC_JSON_FILE_PATH)
    return table.create_table(json_data["value"])


def filter_and_sort_traffic_data(data):
    """Filter and sort the traffic data"""
    max_rate = 5.0
    both_genders = "BTSX"
    table.filter_table_by_column(data, RATE_KEY, "<", max_rate)
    table.filter_table_by_column(data, GENDER_KEY, "==", both_genders)
    table.sort_table_by_column(data, YEAR_KEY, False)
    return data


def get_latest_data_by_country(data):
    """Get the latest data for each country"""
    country_key = COUNTRY_KEY
    data = table.group_table_by_column(data, country_key)
    latest_data_by_country = []
    for group in data:
        first_row = table.pop_table_column(group)
        latest_data_by_country.append(first_row)
    return latest_data_by_country


def create_work_item_payload(traffic_data):
    """Create the API payload for each item"""
    payloads = []
    for row in traffic_data:
        payload = dict(
            country=row[COUNTRY_KEY], year=row[YEAR_KEY], rate=row[RATE_KEY]
        )
        payloads.append(payload)
    return payloads

def save_work_item_payloads(payloads):
    for payload in payloads:
        variables=dict(
            traffic_data=payload
        )
        workitems.outputs.create(variables)
        
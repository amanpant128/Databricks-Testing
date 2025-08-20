# Databricks notebook source
import os
import json
import time
import requests
import tempfile

from datetime import datetime, timedelta
import datetime
from pprint import pprint

# COMMAND ----------

# Define the widget for filter_url_selection
dbutils.widgets.text("filter_url_selection", "", "Filter URL Selection")
dbutils.widgets.text("filter_id", "", "Filter ID")
dbutils.widgets.text("environment", "", "environment")

# Now you can access the widget value
filter_url_selection = dbutils.widgets.get("filter_url_selection")
filter_id = dbutils.widgets.get("filter_id")
environment = dbutils.widgets.get("environment")

# Print the value to verify
# print(f"Filter URL Selection: {filter_url_selection}")

# COMMAND ----------

def load_pem_certificate(pem_path):
    """
    Loads the PEM certificate.

    Parameters:
    pem_path (str): The path to the PEM certificate file.
    
    Returns:
    str: The path to the PEM certificate file if it exists.

    Raises:
    RuntimeError: If the PEM certificate file does not exist or if any other error occurs.
    """
    try:
        if not os.path.exists(pem_path):
            raise RuntimeError(f"Failed to load PEM certificate: {pem_path} does not exist")
        return pem_path
    except Exception as e:
        raise RuntimeError(f"Failed to load PEM certificate: {str(e)}")

# COMMAND ----------

def _get_access_token(pem_file):
    """
    Fetches a new access token using client credentials and certificate files.

    This function attempts to fetch an access token by making a POST request to a token URL.

    Parameters:
    pem_file (str): The path to the PEM certificate file used for authentication.

    Returns:
    str: The access token if the request is successful.

    Raises:
    RuntimeError: If the request fails after the specified number of retries or if any other error occurs.
    """
    try:
        response = requests.post(
            token_url,
            cert=pem_file,
            headers=headers,
            data=client_payload
        )
        
        if response.status_code == 200:
            token_data = response.json()
            print(f"Access token response status: {token_url} is {response.status_code}")
            return token_data['access_token']
        else:
            raise RuntimeError(f"Failed to get access token: {response.status_code}, {response.text}")
    except Exception as e:
        raise RuntimeError(f"Error fetching access token: {str(e)}")

# COMMAND ----------

def fetch_filtered_data(filter_url, pem_file, headers, retry_on_token_expiration=True, retry_count=0):
    """
    Fetches filtered data based on the provided filter URL and handles token expiration.

    This function sends a GET request to the specified filter URL to fetch filtered data. 
    It handles token expiration by attempting to refresh the access token and retrying the request.

    Parameters:
    filter_url (str): The URL to fetch the filtered data from.
    pem_file (str): The path to the PEM certificate file used for authentication.
    headers (dict): The headers to include in the GET request.
    retry_on_token_expiration (bool, optional): Whether to retry the request if the token expires. Defaults to True.
    retry_count (int, optional): The current retry count. Defaults to 0.

    Returns:
    dict: The JSON response from the GET request if successful.

    Raises:
    RuntimeError: If the request fails after the specified number of retries, if the token cannot be refreshed, or if any other error occurs.
    """
    max_retries = 1  # Limiting retry count
    try:
        response = requests.get(filter_url, cert=pem_file, headers=headers)
        print(f"Filter Id response status code: {filter_url} is {response.status_code}")
        response.raise_for_status()  # This will raise an HTTPError for bad responses
        return response.json()  # Assuming the response is a JSON list of attachments
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 401 and retry_on_token_expiration and retry_count < max_retries:  # Token expired, refresh token and retry
            print("Token expired. Fetching new access token")
            access_token_value = _get_access_token(pem_file)
            headers['Authorization'] = f"Bearer {access_token_value}"
            return fetch_filtered_data(filter_url, pem_file, headers, retry_on_token_expiration=False, retry_count=retry_count+1)
        else:
            raise RuntimeError(f"Failed to fetch filtered data: {http_err}, {response.status_code}, {response.text}")
    except Exception as err:
        raise RuntimeError(f"An error occurred: {err}")

# COMMAND ----------

# Function to extract ticket numbers from filtered data
def extract_ticket_numbers_and_status(filtered_data):
    """
    Extracts ticket numbers from the filtered data.

    This function takes filtered data, typically a dictionary containing a list of issues,
    and extracts the ticket numbers from each issue.

    Parameters:
    filtered_data (dict): The filtered data containing issues. Expected to have a key 'Issues' which is a list of dictionaries.

    Returns:
    list: A list of ticket numbers extracted from the filtered data.
    """
    ticket_info = []
    for issue in filtered_data.get('Issues', []):
        # Logic to extract all values
        issue_data = {value['Key']: value['Value'] for value in issue['Values']}
        issue_data['Id'] = issue.get('Id')
        issue_data['Key'] = issue.get('Key')

        # Logic to ensure 'Fund' and 'Status' are specifically captured
        ticket_data = {'Key': issue.get('Key')}
        for value in issue.get('Values', []):
            if value.get('Key') in ['Fund', 'Status']:
                ticket_data[value.get('Key')] = value.get('Value')

        # Combine the results
        combined_data = {**ticket_data, 'ticket_metadata': issue_data}
        ticket_info.append(combined_data)
    return ticket_info

# COMMAND ----------

def fetch_metadata(metadata_url, pem_file, headers, retry_on_token_expiration=True, retry_count=0):
    """
    Fetches metadata and handles token expiration.

    This function sends a GET request to the specified metadata URL to fetch metadata.
    It handles token expiration by attempting to refresh the access token and retrying the request.
    
    Parameters:
    metadata_url (str): The URL to fetch the metadata from.
    pem_file (str): The path to the PEM certificate file used for authentication.
    headers (dict): The headers to include in the GET request.
    retry_on_token_expiration (bool, optional): Whether to retry the request if the token expires. Defaults to True.
    retry_count (int, optional): The current retry count. Defaults to 0.

    Returns:
    dict: The JSON response from the GET request if successful.

    Raises:
    RuntimeError: If the request fails after the specified number of retries, if the token cannot be refreshed, or if any other error occurs.
    """
    max_retries = 1 # Limiting retry count
    try:
        response = requests.get(metadata_url, cert=pem_file, headers=headers)
        print(f"Metadata response status code: {metadata_url} is {response.status_code}")
        response.raise_for_status()  # This will raise an HTTPError for bad responses
        return response.json()  # Assuming the response is a JSON list of attachments
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 401 and retry_on_token_expiration and retry_count < max_retries:  # Token expired, refresh token and retry
            print("Token expired. Fetching new access token")
            access_token_value = _get_access_token(pem_file)
            headers['Authorization'] = f"Bearer {access_token_value}"
            return fetch_metadata(metadata_url, pem_file, headers, retry_on_token_expiration=False, retry_count=retry_count+1)
        else:
            raise RuntimeError(f"Failed to fetch metadata: {http_err}, {response.status_code}, {response.text}")
    except Exception as err:
        raise RuntimeError(f"An error occurred: {err}")

# COMMAND ----------

def process_and_download_attachments(download_url, attachments, mounted_path, pem_file, headers, update_url, jira_ticket_no,fund_name, catalog_name, schema_name, table_name, retry_on_token_expiration=True):
    """
    Processes and downloads each attachment, handling token expiration and network errors.

    This function iterates over a list of attachments, downloads each one, and saves it to the specified mounted path.
    It handles token expiration by attempting to refresh the access token and retrying the download.

    Parameters:
    download_url (str): The base URL for downloading attachments.
    attachments (list): A list of dictionaries, each containing 'Id', 'Name', and 'Size' of the attachment.
    mounted_path (str): The path where the attachments will be saved.
    pem_file (str): The path to the PEM certificate file used for authentication.
    headers (dict): The headers to include in the GET request.
    retry_on_token_expiration (bool, optional): Whether to retry the request if the token expires. Defaults to True.

    Returns:
    list: A list of dictionaries, each containing the 'Id', 'Name', 'Extension', 'Status', and 'Size' of the attachment.

    Raises:
    RuntimeError: If the request fails after the specified number of retries, if the token cannot be refreshed, or if any other error occurs.
    """
    attachment_data = []
    max_retries = 1 # Limiting ertry count

    # Fetch existing processed attachment_ids from Delta Table
    existing_attachment_ids = set()
    
    try:
        existing_df = spark.sql(f"SELECT `attachment_id` FROM `{catalog_name}`.{schema_name}.{table_name}")
        existing_attachment_ids = {row['attachment_id'] for row in existing_df.collect()}
        print(f"Found {len(existing_attachment_ids)} previously processed attachments.")
    except Exception as e:
        raise Exception(f"Error fetching existing attachment IDs: {e}")

    for attachment in attachments:
        attachment_id = attachment['Id']
        attachment_name = attachment['Name']
        extension = os.path.splitext(attachment_name)[1]
        size = attachment['Size']
        download_url_with_id = f"{download_url}&attachmentId={attachment_id}"

        # Skip download if attachment_id has already been processed
        if attachment_id in existing_attachment_ids:
            print(f"Skipping already processed attachment: {attachment_id}")
            load_status_skipped = "Success"  # Assuming the status is "Success" for already processed attachments

            # Update ticket status if load_status_skipped is "Success"
            if load_status_skipped == "Success":
                try:
                    update_response = requests.post(update_url, cert=pem_file, headers=headers_update_ticket, files=files)
                    # Check the status code and handle the response
                    print(f"Update ticket status URL {update_url} response status code: {update_response.status_code}")
                    print("Ticket update for: ", update_url.split('/')[-1])
                    pprint(f"Response Body: {update_response.text}")
                except RuntimeError as e:
                    raise RuntimeError(f"Error updating ticket status: {e}")
            continue

        retries = 0
        load_status = "Failed"  # Default status is "Failed"
        while retries < max_retries:
            try:
                # Download the attachment
                response = requests.get(download_url_with_id, cert=pem_file, headers=headers)
                response.raise_for_status()  # Raise error for bad responses
                print(f"Download URL response status code: {download_url_with_id} is {response.status_code}")
                print(f"Downloading attachment {attachment_name} ({size} bytes)")
                
                # Save the attachment
                download_timestamps = {}
                current_date_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                new_attachment_name = f"{attachment_id}_{attachment_name}"
                file_path = os.path.join(mounted_path, new_attachment_name)
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                print("Attachment fetched successfully.")
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                download_timestamps[attachment['Name']] = current_time
                print(download_timestamps)
                load_status = "Success"

                # Update ticket status if load_status is "Success"
                if load_status == "Success":
                    try:
                        update_response = requests.post(update_url, cert=pem_file, headers=headers_update_ticket, files=files)
                        # Check the status code and handle the response
                        print(f"Update ticket status URL {update_url} response status code: {update_response.status_code}")
                        print("Ticket update for: ", update_url.split('/')[-1])
                        pprint(f"Response Body: {update_response.text}")
                    except RuntimeError as e:
                        raise RuntimeError(f"Error updating ticket status: {e}")

                break
            
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 401 and retry_on_token_expiration and retries < max_retries:  # Token expired, refresh token and retry
                    print("Token expired. Fetching new access token")
                    access_token_value = _get_access_token(pem_file)
                    headers['Authorization'] = f"Bearer {access_token_value}"
                    return process_and_download_attachments(download_url, attachments, mounted_path, pem_file, headers, update_url, jira_ticket_no,fund_name, catalog_name, schema_name, table_name, retry_on_token_expiration=False)
                    retries += 1
                    break
                else:
                    load_status = f"Failed to download: {http_err}, {response.text}"
                    print(load_status)
                break
            except Exception as err:
                load_status = f"Error downloading {attachment_name}: {err}"
                print(load_status)
                break

        attachment_data.append({
            "attachment_id": attachment_id,
            "name": new_attachment_name,
            "extension": extension,
            "load_status": load_status,
            "size": size,
            "download_time": current_time
        })

    return attachment_data

# COMMAND ----------

# Main execution
def main_execution():

    all_attachment_data = []

    # Load certificate and key files
    try:
        pem_file = load_pem_certificate(pem_path)
    except RuntimeError as e:
        print(f"Error loading PEM certificate: {e}")
        return

    # Fetch and update access token in headers
    try:
        access_token = _get_access_token(pem_file)
        headers['Authorization'] = f"Bearer {access_token}"
        headers_update_ticket['Authorization'] = f"Bearer {access_token}"
        print("Access token fetched successfully.")
        print("------------------------------------------------------------------------------------------------------")
    except RuntimeError as e:
        print(f"Error fetching access token: {e}")
        raise RuntimeError(f"Error fetching access token: {e}")
        return
    
    # Fetch filtered data
    try:
        filtered_data = fetch_filtered_data(filter_url, pem_file, headers)
        print("Filtered data fetched successfully.")
    except RuntimeError as e:
        print(f"Error fetching filtered data: {e}")
        raise RuntimeError(f"Error fetching filtered data: {e}")
        return
    
    # Extract ticket numbers from filtered data
    jira_ticket_info_list = extract_ticket_numbers_and_status(filtered_data)
    # print(jira_ticket_info_list)
    print(f"Extracted {len(jira_ticket_info_list)} JIRA ticket numbers.")

    print("------------------------------------------------------------------------------------------------------")

    catalog_name = config[environment]['catalog']
    schema_name = config[environment]['schema']
    table_name = config['tables']['logs']

    # Iterate through each ticket and process
    for jira_ticket_info in jira_ticket_info_list:
    # for jira_ticket_info in filtered_tickets:
        jira_ticket_no = jira_ticket_info['Key']
        jira_ticket_status = jira_ticket_info['Status']
        fund_name = jira_ticket_info['Fund']
        issue_data = jira_ticket_info['ticket_metadata']
        print(f"Processing JIRA ticket: {jira_ticket_no} with status: {jira_ticket_status}")

        metadata_url = config[environment]['rest_apis_url']['metadata_url'].format(jira_ticket_no=jira_ticket_no)
        download_url = config[environment]['rest_apis_url']['download_url'].format(jira_ticket_no=jira_ticket_no)
        update_url = config[environment]['rest_apis_url']['update_url'].format(jira_ticket_no=jira_ticket_no)

        # Fetch attachment metadata and download files
        try:
            attachments = fetch_metadata(metadata_url, pem_file, headers)
            print("Metadata fetched successfully.")
        except RuntimeError as e:
            print(f"Error processing attachments: {e}")
            raise RuntimeError(f"Error processing attachments: {e}")

        try:
            attachment_data = process_and_download_attachments(download_url, attachments, mounted_path, pem_file, headers, update_url, jira_ticket_no, fund_name, catalog_name, schema_name, table_name)
            for data in attachment_data:
                data['ticket_number'] = jira_ticket_no
                data['ticket_status'] = jira_ticket_status
                data['fund'] = fund_name
                data['ticket_metadata'] = issue_data
            if attachment_data:
                # Create and display the dataframe
                df = spark.createDataFrame(attachment_data)
                df = df.select('attachment_id','name','extension','load_status','size','download_time','ticket_number','ticket_status','fund','ticket_metadata')
                df.write \
                    .mode("append") \
                    .format("delta") \
                    .insertInto(f"`{catalog_name}`.{schema_name}.{table_name}")

                df.display()
            # all_attachment_data.extend(attachment_data)
            # print("Attachment fetched successfully.")
            print("------------------------------------------------------------------------------------------------------")

        except RuntimeError as e:
            print(f"Error downloading attachments: {e}")
            raise RuntimeError(f"Error downloading attachments: {e}")
   
    
  

# COMMAND ----------

if __name__=="__main__":

    with open('config.json') as config_file:
        config = json.load(config_file)

    # Authentication details for the Certificates
   
    scope = config[environment]['secrets']['scope']
    key=config[environment]['certificate']

    pem_certificate = dbutils.secrets.get(scope=scope, key=key)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as temp_file:
        temp_file_path = temp_file.name
        temp_file.write(pem_certificate.encode('utf-8'))
    pem_path = temp_file.name


    # Token URL
    token_url = config[environment]['rest_apis_url']['token_url']
    
    # credentials
    username = dbutils.secrets.get(scope=scope, key=config[environment]['secrets']['username'])
    password = dbutils.secrets.get(scope=scope, key=config[environment]['secrets']['password'])
    app_key = dbutils.secrets.get(scope=scope, key=config[environment]['secrets']['app_key'])
    authorization = dbutils.secrets.get(scope=scope, key=config[environment]['secrets']['authorization'])

    # filter URL
    filter_url = config[environment]['rest_apis_url']['filter_url'].format(filter_id=filter_id)

    # Determinining the filter URL and mounted path based on the parameter value
    if filter_url_selection == "prelim":
        print("Filtering the prelim files")
        mounted_path = config[environment]['volumes']['spvfilestore_prelim']
        update_ticket = config[environment]['update_ticket_preliminary_rest_api_payload']
    elif filter_url_selection == "final":
        print("Filtering the final files")
        mounted_path = config[environment]['volumes']['spvfilestore_final']
        update_ticket = config[environment]['update_ticket_final_rest_api_payload']

    # Payload and headers for token generation
    client_payload = {
        'username': username,
        'password': password,
        'grant_type': 'client_credentials'
    }

    headers = {
        'content_type': 'application/x-www-form-urlencoded',
        'AppKey': app_key,
        'Authorization': authorization
    }

    # Headers and Payloads for updating ticket rest api
    headers_update_ticket = {
    'AppKey': app_key,
    'Authorization': authorization
    }
    ticket_data = {"fields": update_ticket}
    files = {'ticketModel': (None, json.dumps(ticket_data), 'application/json')}

    main_execution()
    os.remove(temp_file_path)

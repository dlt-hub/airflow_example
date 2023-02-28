import json
import requests

# Set the Jira URL
#url = "https://dltjireapipeline.atlassian.net/rest/api/2/issue/createmeta"
url = "https://dltjireapipeline.atlassian.net/rest/api/2/search?jql=issuetype = Epic AND project = AMS"

# Set the authentication details
auth = ("amanguptanalytics@gmail.com", "API Token")

# Set the headers
headers = {
    "Accept": "application/json"
}

# Make the request using the GET method
response = requests.get(url, auth=auth, headers=headers)
json_response = response.json()
# Check the response status code
if response.status_code == 200:
    # If the request was successful (status code 200), print the response text
    print(json.dumps(json_response, indent=4))
else:
    # If the request failed, print the error status code
    print("Request failed with status code:", response.status_code)

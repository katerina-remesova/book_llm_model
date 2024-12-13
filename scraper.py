import requests
import json
import time

# Load the JSON data from a file
json_file = "response_vedlejska.json"  # Replace with your JSON file path
output_dir = "./output/downloaded_html"  # Directory to save the downloaded HTML files  

# Headers for requests
cookies = {
    "UISAuth":"",
    "aad_idp_id":"2"
}


# Read the JSON file
with open(json_file, "r") as file:
    data = json.load(file)

# Ensure the output directory exists
import os
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Read the JSON file
with open(json_file, "r") as file:
    data = json.load(file)

# Iterate over all keys in the JSON
for group, items in data.items():
    if not isinstance(items, list):
        continue  # Skip non-list groups

    print(f"Processing group: {group}")
    for subject in items:
        # Ensure the subject has "code" and "code_link"
        if "code" not in subject or "code_link" not in subject:
            continue

        code = subject["code"]
        code_link = subject["code_link"]

        print(f"Fetching HTML for subject: {code} from {code_link}")
        try:
            # Perform the GET request with cookies
            response = requests.get(code_link, cookies=cookies)
            response.raise_for_status()  # Raise an error for HTTP errors

            # Save the HTML content to a file
            output_file = os.path.join(output_dir, f"{code}.html")
            with open(output_file, "w", encoding="utf-8") as html_file:
                html_file.write(response.text)
            
            print(f"Saved HTML for {code} to {output_file}")
        except requests.RequestException as e:
            print(f"Failed to fetch HTML for {code}: {e}")
        
        # Sleep for 5 seconds between requests
        time.sleep(5)

print("All pages have been downloaded.")

import linecache
import json

# Make sure that requests is installed in your WSL
import requests

# We could just read the entire file, but if it's really big you could go line by line
# If you want make this an exercise and replace the process below by reading the whole file at once and going line by line

# set starting id and ending id
start = 1
end = 15

# Loop over the JSON file
i = start

while i <= end:
    # read a specific line
    line = linecache.getline('../data/output.txt', i)
    print(line)
    # write the line to the API
    myjson = json.loads(line)

    print(myjson)

    response = requests.post('http://localhost:80/invoice-item', json=myjson)

    # Use this for debugging
    print("Status code: ", response.status_code)
    print("Printing Entire Post Request")
    print('Error:', response.reason)
    print(response.json())

    # increase i
    i += 1
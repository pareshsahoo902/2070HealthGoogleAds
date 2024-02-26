import json

def count_objects_in_json_file(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)

        # Check if the top-level structure is a list
        if isinstance(data, list):
            number_of_objects = len(data)
            print(f'The JSON file contains {number_of_objects} objects.')
        else:
            print('The top-level structure is not a list. Unable to determine the number of objects.')

# Replace 'your_json_file.json' with the actual path to your JSON file
json_file_path = 'your_json_file.json'
count_objects_in_json_file('data/google_ads_data.json')
import json
import argparse
import os
from multiprocessing import Pool

# Argument parser setup
parser = argparse.ArgumentParser()
parser.add_argument('--output', type=str, required=True, help='Output folder for chunked JSON files')
parser.add_argument('--filename', type=str, required=True, help='Name of the BloodHound JSON file')
parser.add_argument('--chunks', type=int, default=100, help='Number of chunks to split the BloodHound JSON file into')
args = parser.parse_args()

def process_chunk(chunk_data):
    output, idx, chunk, meta = chunk_data
    new_file = {
        "data": chunk,
        "meta": meta
    }
    write_json(new_file, os.path.join(output, f'chunk_{idx}.json'))

def main(args):
    # Ensure output folder exists
    os.makedirs(args.output, exist_ok=True)

    # Import JSON data
    data = import_json(args.filename)

    # Prepare meta data
    count, data_type, version = data['meta']['count'], data['meta']['type'], data['meta']['version']
    meta = {"type": data_type, "version": version, "count": 0}

    # Generate chunks
    chunks = json_chunks(data, args.chunks)

    # Prepare data for multiprocessing
    chunk_data_for_processing = [(args.output, idx, chunk, {**meta, "count": len(chunk)}) for idx, chunk in enumerate(chunks)]

    # Use multiprocessing to process chunks
    with Pool() as pool:
        pool.map(process_chunk, chunk_data_for_processing)

def write_json(data, filename):
    try:
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)
    except IOError as e:
        print(f"Error writing file {filename}: {e}")

def import_json(filename):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as e:
        print(f"Error reading file {filename}: {e}")
        return None

def json_chunks(data, chunks):
    chunk_size = max(1, len(data['data']) // chunks)
    return [data['data'][i:i + chunk_size] for i in range(0, len(data['data']), chunk_size)]

if __name__ == '__main__':
    main(args)

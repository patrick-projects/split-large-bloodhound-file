import json
import argparse
import os
from multiprocessing import Pool

# Argument parser setup
parser = argparse.ArgumentParser()
parser.add_argument('--output', type=str, required=True, help='Output folder for chunked JSON files')
parser.add_argument('--filename', type=str, required=True, help='Name of the BloodHound JSON file')
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
    meta = {"type": data['meta']['type'], "version": data['meta']['version'], "count": 0}

    # Estimate chunk size (1GB)
    target_chunk_size_in_bytes = 1 * 1024 * 1024 * 1024  # 1GB in bytes

    # Generate chunks
    chunks = json_chunks(data, target_chunk_size_in_bytes)

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

def json_chunks(data, target_chunk_size_in_bytes):
    current_chunk = []
    current_chunk_size = 0
    chunks = []

    for item in data['data']:
        item_size = len(json.dumps(item))  # Estimate size of the item
        if current_chunk_size + item_size > target_chunk_size_in_bytes and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_chunk_size = 0
        current_chunk.append(item)
        current_chunk_size += item_size

    if current_chunk:  # Add the last chunk if it has data
        chunks.append(current_chunk)

    return chunks

if __name__ == '__main__':
    main(args)

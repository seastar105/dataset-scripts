import json
import argparse
from pathlib import Path
import ray


def convert(sample):
    audio_path = sample.pop("audio_path")
    key = Path(audio_path).stem
    with open(audio_path, "rb") as f:
        audio_bytes = f.read()
    audio_ext = Path(audio_path).suffix[1:]
    return {
        "__key__": key,
        audio_ext: audio_bytes,
        "json": json.dumps(sample, ensure_ascii=False)
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Write to a webdataset')
    
    parser.add_argument("--input_jsonl", type=str, required=True, help="Path to the input jsonl file")
    parser.add_argument("--output_root", type=str, required=True, help="Path to the output root directory")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle the dataset")
    parser.add_argument("--min_rows_per_file", type=int, default=10000, help="Minimum number of rows per file")
    parser.add_argument("--compress", action="store_true", help="Compress the dataset")
    
    args = parser.parse_args()
    
    with open(args.input_jsonl, "r") as f:
        data = [json.loads(line) for line in f]
    
    Path(args.output_root).mkdir(parents=True, exist_ok=True)
    dataset = ray.data.from_items(data).random_shuffle().map(convert).write_webdataset(args.output_root, min_rows_per_file=args.min_rows_per_file, compress=args.compress)

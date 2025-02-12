import json
import argparse
from pathlib import Path
import ray
import io
from pydub import AudioSegment
from functools import partial


def convert(sample):
    audio_path = sample.pop("audio_path")
    if "__key__" in sample:
        key = sample.pop("__key__")
    else:
        key = Path(audio_path).stem
    with open(audio_path, "rb") as f:
        audio_bytes = f.read()
    audio_ext = Path(audio_path).suffix[1:]
    return {
        "__key__": key,
        audio_ext: audio_bytes,
        "json": json.dumps(sample, ensure_ascii=False)
    }


def segment_convert(sample, **audio_export_kwargs):
    audio_path = sample.pop("audio_path")
    if "__key__" in sample:
        key = sample.pop("__key__")
    else:
        key = Path(audio_path).stem
    
    start = sample.pop("start")
    end = sample.pop("end")
    duration = end - start
    
    # To save my storage, use mp3 with 128k bitrate
    segment = AudioSegment.from_file(audio_path, start=start, duration=duration)
    audio_bytes = io.BytesIO()
    segment.export(audio_bytes, **audio_export_kwargs)
    audio_bytes = audio_bytes.getvalue()
    
    return {
        "__key__": key,
        "mp3": audio_bytes,
        "json": json.dumps(sample, ensure_ascii=False)
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Write to a webdataset')
    
    parser.add_argument("--input_jsonl", type=str, required=True, help="Path to the input jsonl file")
    parser.add_argument("--output_root", type=str, required=True, help="Path to the output root directory")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle the dataset")
    parser.add_argument("--min_rows_per_file", type=int, default=10000, help="Minimum number of rows per file")
    parser.add_argument("--compress", action="store_true", help="Compress the dataset")
    parser.add_argument("--segment", action="store_true", help="Segment is specified in the input jsonl file")
    parser.add_argument("--save_mp3", action="store_true", help="Save mp3 instead of wav")
    
    args = parser.parse_args()
    
    with open(args.input_jsonl, "r") as f:
        data = [json.loads(line) for line in f]
    
    Path(args.output_root).mkdir(parents=True, exist_ok=True)
    if args.save_mp3:
        audio_export_kwargs = {"format": "mp3", "bitrate": "128k"}
    else:
        audio_export_kwargs = {"format": "wav"}
    segment_convert = partial(segment_convert, **audio_export_kwargs)
    convert_fn = segment_convert if args.segment else convert
    
    dataset = ray.data.from_items(data).random_shuffle().map(convert_fn).write_webdataset(args.output_root, min_rows_per_file=args.min_rows_per_file, compress=args.compress)

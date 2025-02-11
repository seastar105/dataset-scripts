import glob
import argparse
import json
import os
from pathlib import Path
from tqdm.auto import tqdm

SPLITS = ["train-clean-100", "train-clean-360", "train-other-500", "dev-clean", "dev-other", "test-clean", "test-other"]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--root", type=str, help="root directory of librispeech")
    
    args = parser.parse_args()
    
    for split in SPLITS:
        txt_files = glob.glob(f"{args.root}/{split}/**/*.trans.txt", recursive=True)
        data = []
        
        for txt_file in tqdm(txt_files, desc=f"Processing {split}..."):
            dir = Path(txt_file).parent
            with open(txt_file, "r") as f:
                for line in f:
                    stem, text = line.strip().split(" ", 1)
                    audio_path = f"{dir}/{stem}.flac"
                    assert os.path.exists(audio_path), f"Error while processing {txt_file}: {line}"
                    spk_id = dir.parts[-2]
                    data.append({
                        "audio_path": audio_path,
                        "spk_id": spk_id,
                        "text": text
                    })
        
        with open(f"{args.root}/{split}.jsonl", "w", encoding="utf-8") as f:
            for sample in data:
                print(json.dumps(sample, ensure_ascii=False), file=f)

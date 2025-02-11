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
        audio_paths = glob.glob(f"{args.root}/{split}/**/*.wav", recursive=True)
        data = []
        
        for audio_path in tqdm(audio_paths, desc=f"Processing {split}..."):
            dir = Path(audio_path).parent
            text_path = f"{dir}/{Path(audio_path).stem}.original.txt"
            tn_text_path = f"{dir}/{Path(audio_path).stem}.normalized.txt"
            
            if not os.path.exists(audio_path):
                print(f"Skipping {audio_path} because it does not exist")
                continue
            if not os.path.exists(text_path):
                print(f"Skipping {text_path} because it does not exist")
                continue
            if not os.path.exists(tn_text_path):
                print(f"Skipping {tn_text_path} because it does not exist")
                continue
            
            with open(text_path) as f:
                text = f.read().strip()
            
            with open(tn_text_path) as f:
                tn_text = f.read().strip()
            
            spk_id = dir.parts[-2]
            data.append({
                "audio_path": audio_path,
                "text": text,
                "tn_text": tn_text,
                "spk_id": spk_id
            })
        
        with open(f"{args.root}/{split}.jsonl", "w", encoding="utf-8") as f:
            for sample in data:
                print(json.dumps(sample, ensure_ascii=False), file=f)

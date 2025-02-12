import json
import glob
import argparse
from pathlib import Path
from tqdm.auto import tqdm
import re

SPLITS = ["Training", "Validation"]

def bracket_filter(sentence, mode='phonetic'):
    new_sentence = str()

    if mode == 'phonetic':
        flag = False

        for ch in sentence:
            if ch == '(' and flag is False:
                flag = True
                continue
            if ch == '(' and flag is True:
                flag = False
                continue
            if ch != ')' and flag is False:
                new_sentence += ch

    elif mode == 'spelling':
        flag = True

        for ch in sentence:
            if ch == '(':
                continue
            if ch == ')':
                if flag is True:
                    flag = False
                    continue
                else:
                    flag = True
                    continue
            if ch != ')' and flag is True:
                new_sentence += ch

    else:
        raise ValueError("Unsupported mode : {0}".format(mode))

    return new_sentence


def special_filter(sentence, mode='phonetic', replace=None):
    SENTENCE_MARK = ['?', '!', '.']
    NOISE = ['o', 'n', 'u', 'b', 'l']
    EXCEPT = ['/', '+', '*', '-', '@', '$', '^', '&', '[', ']', '=', ':', ';', ',']

    new_sentence = str()
    for idx, ch in enumerate(sentence):
        if ch not in SENTENCE_MARK:
            if idx + 1 < len(sentence) and ch in NOISE and sentence[idx + 1] == '/':
                continue

        if ch == '#':
            new_sentence += '샾'

        elif ch == '%':
            if mode == 'phonetic':
                new_sentence += replace
            elif mode == 'spelling':
                new_sentence += '%'

        elif ch not in EXCEPT:
            new_sentence += ch

    pattern = re.compile(r'\s\s+')
    new_sentence = re.sub(pattern, ' ', new_sentence.strip())
    return new_sentence

def sentence_filter(raw_sentence, mode, replace=None):
    return special_filter(bracket_filter(raw_sentence, mode), mode, replace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--root", type=str, help="root directory of aihub_71556")
    
    args = parser.parse_args()
    
    for split in SPLITS:
        audio_paths = glob.glob(f"{args.root}/{split}/**/*.wav", recursive=True)
        json_paths = []
        for audio_path in tqdm(audio_paths, desc="Check meta path"):
            json_path = Path(audio_path.replace("01.원천데이터", "02.라벨링데이터")).with_suffix(".json")
            json_paths.append(json_path)
        data = []
        for audio_path, json_path in tqdm(zip(audio_paths, json_paths), desc="Load meta data"):
            with open(json_path, "r") as f:
                item = json.load(f)
                
            dialogs = sorted(item["dialogs"], key=lambda x: x["dialogID"])
            text = " ".join([sentence_filter(utt["textOrigin"], mode="spelling") for utt in dialogs])
            tn_text = " ".join([sentence_filter(utt["text"], mode="phonetic") for utt in dialogs])
            seg_text = "|".join([sentence_filter(utt["textOrigin"], mode="spelling") for utt in dialogs])
            seg_tn_text = "|".join([sentence_filter(utt["text"], mode="phonetic") for utt in dialogs])
            data.append({
                "audio_path": audio_path,
                "text": text,
                "tn_text": tn_text,
                "seg_text": seg_text,
                "seg_tn_text": seg_tn_text
            })
        
        with open(f"{args.root}/{split}.jsonl", "w", encoding="utf-8") as f:
            for item in data:
                print(json.dumps(item, ensure_ascii=False), file=f)

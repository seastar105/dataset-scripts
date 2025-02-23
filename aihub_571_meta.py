# from https://github.com/sooftware/ksponspeech/blob/master/preprocess/preprocess.py
import re
import glob
import argparse
from pathlib import Path
import json
from tqdm.auto import tqdm


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
                new_sentence += "퍼센트"
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
    
    parser.add_argument("--audio_root", required=True, type=str)
    parser.add_argument("--output_path", required=True, type=str)
    
    args = parser.parse_args()
    
    audio_paths = sorted(glob.glob(f"{args.audio_root}/**/*.wav", recursive=True))
    samples = []
    for audio_path in tqdm(audio_paths):
        txt_path = audio_path.replace("wav", "txt").replace("원천데이터", "라벨링데이터")
        with open(txt_path, "r", encoding="utf-8") as f:
            raw_text = f.read().strip()
        if "#" in raw_text or "xx" in raw_text:
            print(raw_text)
            continue
        text = sentence_filter(raw_text, "spelling")
        tn_text = sentence_filter(raw_text, "phonetic")
        samples.append({
            "audio_path": audio_path,
            "text": text,
            "tn_text": tn_text,
            "raw_text": raw_text
        })

    with open(args.output_path, "w", encoding="utf-8") as f:
        for sample in samples:
            print(json.dumps(sample, ensure_ascii=False), file=f)

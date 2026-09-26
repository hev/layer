#!/usr/bin/env python3
"""Validation only: pinned Transformers CPU reference; never part of runtime."""
import argparse
import json
from pathlib import Path
import torch
from transformers import AutoModel, AutoTokenizer

parser = argparse.ArgumentParser()
parser.add_argument('models', type=Path)
parser.add_argument('output', type=Path)
args = parser.parse_args()
torch.set_num_threads(1)
manifest = json.loads((args.models / 'manifest.json').read_text())
texts = ['Red shoes for walking through the city.', 'The telescope discovered a distant blue galaxy.']
cases = []
for record in manifest['models']:
    folder = args.models / Path(next(f['path'] for f in record['files'] if f['path'].endswith('/model.safetensors'))).parent
    model = AutoModel.from_pretrained(folder, local_files_only=True, trust_remote_code=False, use_safetensors=True, attn_implementation='eager').eval()
    tokenizer = AutoTokenizer.from_pretrained(folder, local_files_only=True, trust_remote_code=False)
    for purpose in ['document', 'query']:
        inputs = [record['prefixes'][purpose] + text for text in texts]
        batch = tokenizer(inputs, padding=True, truncation=False, return_tensors='pt')
        with torch.no_grad():
            hidden = model(**batch).last_hidden_state
            if record['pooling'] == 'cls':
                vectors = hidden[:, 0]
            else:
                mask = batch['attention_mask'].unsqueeze(-1).float()
                vectors = (hidden * mask).sum(1) / mask.sum(1)
            vectors = torch.nn.functional.normalize(vectors, p=2, dim=1)
        cases.append(dict(model=record['id'], purpose=purpose, inputs=inputs, vectors=vectors.tolist(), tokens=int(batch['attention_mask'].sum())))
args.output.write_text(json.dumps(dict(torch=torch.__version__, cases=cases), indent=2) + '\n')

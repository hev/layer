#!/usr/bin/env python3
"""Build-time only downloader. Runtime has no downloader or HF client."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import urllib.request

PACKAGE = Path(__file__).resolve().parent.parent

def prepare(destination):
    lock = json.loads((PACKAGE / 'artifacts.lock.json').read_text())
    manifest_bytes = (PACKAGE / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    assert lock['version'] == manifest['version'] == 1
    expected = {f['path']: f for m in manifest['models'] for f in m['files']}
    destination.mkdir(parents=True, exist_ok=True)
    seen = set()
    for model in lock['models']:
        assert len(model['revision']) == 40 and all(c in '0123456789abcdef' for c in model['revision'])
        for artifact in model['files']:
            relative = Path(artifact['path'])
            assert not relative.is_absolute() and '..' not in relative.parts
            assert artifact['path'] not in seen
            seen.add(artifact['path'])
            for key in ['path', 'size', 'sha256']:
                assert artifact[key] == expected[artifact['path']][key]
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                temporary = target.with_name(target.name + '.partial')
                if 'url' in artifact:
                    assert artifact['url'].startswith(f"https://huggingface.co/{model['id']}/resolve/{model['revision']}/")
                    with urllib.request.urlopen(artifact['url'], timeout=120) as response, temporary.open('wb') as output:
                        shutil.copyfileobj(response, output)
                else:
                    source = PACKAGE / artifact['source']
                    assert source.resolve().is_relative_to(PACKAGE)
                    shutil.copyfile(source, temporary)
                temporary.replace(target)
            hasher = hashlib.sha256()
            with target.open('rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    hasher.update(block)
            digest = hasher.hexdigest()
            if target.stat().st_size != artifact['size'] or digest != artifact['sha256']:
                raise ValueError(f"integrity mismatch: {model['id']} {relative}")
    assert seen == set(expected)
    (destination / 'manifest.json').write_bytes(manifest_bytes)
    print('Verified both pinned stock bundles; runtime preparation complete.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('destination', type=Path)
    prepare(parser.parse_args().destination)

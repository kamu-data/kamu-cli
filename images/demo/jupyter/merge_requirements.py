#!/usr/bin/env python

import sys
import yaml

env_path = sys.argv[1]
req_path = sys.argv[2]

# Read files
with open(env_path) as f:
    env = yaml.safe_load(f)

with open(req_path) as f:
    reqs = [r.strip() for r in f.readlines()]

# Filter out pip packages from `conda env export`
env['dependencies'] = [
    dep for dep in env['dependencies']
    if not isinstance(dep, dict) or 'pip' not in dep
]

# Filter conda packages from `pip freeze` output
reqs = [r for r in reqs if not '@ file://' in r]

# Merge into environment
env['dependencies'].append({'pip': reqs})

# Replace env file
with open(env_path, 'w') as f:
    yaml.safe_dump(env, f)

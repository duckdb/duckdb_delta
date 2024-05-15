#!/usr/bin/env python3
import sys
import glob
import subprocess
import os

# Get the directory and construct the patch file pattern
directory = sys.argv[1]
patch_pattern = f"{directory}*.patch"

# Early out if the patches were applied here before
DUCKDB_APPLIED_PATCHES_FILE = './applied_duckdb_patches'
if os.path.isfile(DUCKDB_APPLIED_PATCHES_FILE):
    sys.exit(0)

# Find patch files matching the pattern
patches = glob.glob(patch_pattern)

import os
def touch(path):
    with open(path, 'a'):
        os.utime(path, None)

def raise_error(error_msg):
    sys.stderr.write(error_message + '\n')
    sys.exit(1)

patches = os.listdir(directory)
for patch in patches:
    if not patch.endswith('.patch'):
        raise_error(
            f'Patch file {patch} found in directory {directory} does not end in ".patch" - rename the patch file'
        )

# Apply each patch file using git apply
for patch in patches:
    print(f"Applying patch: {patch}\n")
    subprocess.run(
        ["git", "apply", "--ignore-space-change", "--ignore-whitespace", os.path.join(directory, patch)], check=True
    )

# Mark patches applied because CMake is wonky
touch(DUCKDB_APPLIED_PATCHES_FILE)
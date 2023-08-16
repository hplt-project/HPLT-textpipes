# This Python script walks over all the subdirectories in the current directory
# and finds files conforming to the mask `scored*preexisting`,
# e.g., scored.10.jsonl.zst-preexisting
# These files appear after merging monotexted files from different crawls/collections
# with a command like:
# rsync -av -b --suffix -preexisting cc40/ one/cleaned/finalized

# Since file names are not unique across crawls, this script is needed
# to rename files with non-unique names, assigning them sequential numbers,
# e.g., renaming scored.10.jsonl.zst-preexisting to scored.50.jsonl.zst


import os
import sys
from os.path import join

langs = [el for el in os.scandir(".") if el.is_dir()]

for lang in langs:
    print(lang.name, file=sys.stderr)
    print("==============", file=sys.stderr)

    files_without_suffix = 0
    with os.scandir(path=lang.name) as allfiles:
        for entry in allfiles:
            if (
                not "preexisting" in entry.name
                and entry.name.startswith("scored")
                and entry.is_file()
            ):
                files_without_suffix += 1
    with os.scandir(path=lang.name) as allfiles:
        for entry in allfiles:
            if (
                "preexisting" in entry.name
                and entry.name.startswith("scored")
                and entry.is_file()
            ):
                files_without_suffix += 1
                curr_number = entry.name.split(".")[1]
                new_number = files_without_suffix
                new_name = entry.name.replace(curr_number, str(new_number)).replace(
                    "-preexisting", ""
                )
                print(f"Renaming {entry.name} to {new_name}", file=sys.stderr)
                os.rename(join(lang.name, entry.name), join(lang.name, new_name))

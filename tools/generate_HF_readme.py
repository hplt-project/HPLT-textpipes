import argparse
import json
from smart_open import open
import logging
import pandas

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Generate HPLT HF dataset card from manifest.json and placeholder file"
    )
    parser.add_argument(
        "--manifest",
        "-m",
        type=str,
        default="manifest.json",
        help="Path to the manifest file",
    )
    parser.add_argument(
        "--readme",
        "-r",
        type=str,
        default="README_placeholder.md",
        help="Path to the README.md file with the placeholders",
    )
    parser.add_argument(
        "--readme_out",
        "-o",
        type=str,
        default="README.md",
        help="Path to the output file",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
    )
    logger = logging.getLogger(__name__)

    our_langs = {}

    with open(args.manifest) as f:
        for el in f:
            entry = json.loads(el.strip())
            our_langs[entry["name"]] = entry
    with open(args.readme) as f:
        readme_text = f.read()

    df = pandas.DataFrame.from_dict(our_langs, orient="index")
    df.index.name = "Language"

    stats_table = df[
        ["bytes", "documents", "segments", "tokens", "characters"]
    ].to_markdown(tablefmt="github")

    language_names = set([our_langs[el]["name"].split("_")[0] for el in our_langs])
    language_names = "\n- ".join(sorted(language_names))

    download_links = [
        f"[{our_langs[el]["name"]}]({our_langs[el]["map"]})" for el in our_langs
    ]
    download_links = "\n- ".join(download_links)

    readme_text = readme_text.replace(
        "{{LANGUAGES_PLACEHOLDER}}", "- " + language_names
    )
    readme_text = readme_text.replace("{{DOWNLOAD_PLACEHOLDER}}", "- " + download_links)
    readme_text = readme_text.replace("{{STATISTICS_PLACEHOLDER}}", stats_table)

    with open(args.readme_out, "w+") as f:
        f.write(readme_text)
    logger.info(f"Content written to {args.readme_out}")

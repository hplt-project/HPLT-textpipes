"""The prototype script for language identification for warc2text-runner."""

from __future__ import annotations

import argparse
import fileinput
import logging

import fasttext
import numpy
import regex
import ujson
import os
import sys;
from pathlib import Path;

from hplt_textpipes.stage3.fastertext_lid.basic_log import langid_logger
from hplt_textpipes.stage3.fastertext_lid.patterns import NONWORD_REPLACE_PATTERN, SPACE_PATTERN


class FastTextLangId:
    """The FastText language identification model."""

    def __init__(
        self,
        model_path: str,
        *,
        use_logging: bool = False,
        level_log: int | None = logging.INFO,
        identity: str = "openlid-v3"
    ) -> None:
        """
        Init the FastText model.

        To download the model, run the following commands:
        wget https://zenodo.org/records/15056559/files/openlid_v2_180325.bin

        Expected usage (stdin jsonlines):
        python -m src.hplt_textpipes.stage2.fastertext_lid.proto_langid --model_path $MODEL_PATH < $YOUR_FILE

        """
        if use_logging is True:
            self.logger = langid_logger(name=f"{identity}_langid_logger", level=level_log)
        else:
            self.logger = logging.getLogger(name=f"{identity}_langid_logger_disabled")
            self.logger.disabled = True
        self.identity = identity;
        self.model = fasttext.load_model(model_path)
        self.logger.debug(f"FastTextLangId model loaded: {model_path}.")

    def _preproccess_text(self, text: str) -> str:
        """Preprocesses a single line of text for lang ID."""
        if not isinstance(text, str):
            msg = "Input text must be a string."
            raise TypeError(msg)

        self.logger.debug("Before: %s", text)
        text = text.replace('\n', ' ').strip().lower()
        text = regex.sub(SPACE_PATTERN, " ", text)
        text = regex.sub(NONWORD_REPLACE_PATTERN, "", text)
        self.logger.debug("After: %s", text)
        return text

    def _postprocess_predicted_labels(self, prediction: tuple) -> list[str]:
        """
        Postprocess the predicted labels.

        Example: "__label__eng_Latn" -> "eng_Latn"
        """
        return [label.replace("__label__", "") for label in prediction[0]]

    def _postprocess_predicted_probabilities(self, prediction: tuple) -> list[float]:
        """
        Postprocess the predicted probabilities.

        Example: [0.92134414] -> [0.9213]
        """
        rounded_probs = numpy.round(prediction[1], decimals=4)

        return rounded_probs.tolist()

    def predict_language_from_stdin_jsonlines(self) -> None:
        """
        Read from stdin jsonlines.

        Example input:

        {"t": "Hello, world!"}

        Example output:

        {"lang": ["eng_Latn"], "prob": [0.9213]}

        """
        with fileinput.input(files=("-",), encoding="utf-8") as f:
            for fileinput_line in f:
                self.logger.debug("Read fileinput line: %s", fileinput_line)
                # load json line
                json_line = ujson.loads(fileinput_line)
                self.logger.debug("Read json line.")

                if json_line["t"] is None:
                    self.logger.debug("Case: text is None.")
                    print(ujson.dumps({"lang": None}))

                elif len(json_line["t"]) == 0:
                    self.logger.debug("Case: text is empty.")
                    print(ujson.dumps({"lang": None}))

                else:
                    self.logger.debug("Case: text is ok.")
                    text = json_line["t"];
                    if self.identity is None or "openlid" in self.identity:
                        text = self._preproccess_text(text);
                    else:
                        text = text.replace("\n", " ");
                    prediction = self.model.predict(
                        text=text,
                        k=3,
                        threshold=0.0,
                        on_unicode_error="strict",
                    )

                    result = { "lang": self._postprocess_predicted_labels(prediction),
                               "prob": self._postprocess_predicted_probabilities(prediction) };
                    if self.identity is not None:
                        result = {self.identity: result};
                    print(ujson.dumps(result));

        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Predict language using FastText model."
    )
    parser.add_argument(
        "--model_path",
        type=str,
        default=None,
        help="Path to the FastText model file.",
    )
    parser.add_argument(
        "--identity",
        type=str,
        default="openlid-v3",
        help="Type and version of LID model.",
    )
    parser.add_argument(
        "--use_logging",
        type=bool,
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Use logging.",
    )

    parser.add_argument(
        "--log_level",
        type=str,
        default="DEBUG",
        help="Logging level.",
    )

    args = parser.parse_args()

    model = args.model_path;
    if model is None:
        base = os.path.dirname(os.path.realpath(__file__));
        model = os.path.join(base, args.identity + ".bin");
    if not os.path.isfile(model):
        base = os.path.join(Path.home(), ".cache", "hplt");
        model = os.path.join(base, args.identity + ".bin");
    if not os.path.isfile(model):
        print("proto_langid.py: missing model file for {}; exit."
              "".format(args.identity),
              file = sys.stderr);
        sys.exit(1);
        
    loaded_model = FastTextLangId(
        model_path=model,
        use_logging=args.use_logging,
        level_log=logging.getLevelName(args.log_level),
        identity = args.identity
    )

    loaded_model.predict_language_from_stdin_jsonlines()

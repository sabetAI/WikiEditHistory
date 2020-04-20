#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import argparse
import logging
import yaml
import json
import html2text

# it may be required if you have installed NLTK locally
# import nltk.data
# nltk.data.path.append('$HOME/nltk_data')

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from wiki_edit_extractor import WikiEditExtractor
from wikiedits import LANGUAGES

from ipdb import set_trace
from tqdm import tqdm

import re
from html.entities import name2codepoint
from transformers import BertTokenizer


def htmlentitydecode(s):
    decoded = re.sub(
        "&(%s);" % "|".join(name2codepoint),
        lambda m: chr(name2codepoint[m.group(1)]),
        s,
    )
    return re.sub(r"^https?:\/\/.*[\r\n]*", "", decoded)


def main():
    args = parse_user_args()

    if args.debug:
        set_logging_level("debug")
    wiki = WikiEditExtractor(
        args.input or sys.stdin,
        lang=args.language,
        min_words=args.min_words,
        max_words=args.max_words,
        length_diff=args.length_diff,
        edit_ratio=args.edit_ratio,
        min_chars=args.min_chars,
    )

    if args.tabify:
        output = "{old}\t{new}\t{prev_ctxt}\t{next_ctxt}\t{rid}\t{timestamp}\t{uid}\t{minor}\t{comment}\t{pid}\t{ratio}\t{dist}\n"
        if args.scores:
            output += "\t{dist}\t{ratio}\n"
    elif args.context:
        output = "{old}\n{new}\n{prev}\n{next}\n\n"
    else:
        output = "{old}\n{new}\n\n"
        if args.scores:
            output = "### scores: {{dist: {dist}, ratio: {ratio}}}\n" + output

    out = args.output
    if args.output != sys.stdout:
        out = open(args.output, "w")

    out.write(
        "old\tnew\tprev_ctxt\tnext_ctxt\trid\ttimestamp\tuid\tminor\tcomment\tpid\tratio\tdist\n"
    )
    html2txt = html2text.HTML2Text()
    html2txt.ignore_links = True
    html2txt.body_width = 0

    for edits, meta in tqdm(wiki.extract_edits()):
        # if args.meta_data and edits:
        #     out.write(format_meta_data(meta) + "\n")

        for (old_edit, new_edit, scores, prev_ctxt, next_ctxt) in edits:
            prev_ctxt = "<SEP>".join(
                [htmlentitydecode(c) for c in prev_ctxt if c != ""]
            ).encode("utf-8")
            next_ctxt = "<SEP>".join(
                [htmlentitydecode(c) for c in next_ctxt if c != ""]
            ).encode("utf-8")
            out.write(
                output.format(
                    old=htmlentitydecode(old_edit).encode("utf-8"),
                    new=htmlentitydecode(new_edit).encode("utf-8"),
                    prev_ctxt=prev_ctxt,
                    next_ctxt=next_ctxt,
                    rid=meta["id"],
                    timestamp=meta["timestamp"],
                    uid=meta["contributor"]["id"]
                    if "id" in meta["contributor"]
                    else meta["contributor"]["ip"]
                    if "ip" in meta["contributor"]
                    else "",
                    minor=meta["minor"],
                    comment=meta["comment"].encode("utf-8")
                    if "comment" in meta and meta["comment"] is not None
                    else "none".encode("utf-8"),
                    pid=meta["page"]["id"],
                    ratio=scores[0],
                    dist=scores[1],
                )
            )

    if args.output != sys.stdout:
        out.close()


def format_meta_data(data):
    lines = [
        "### %s" % line
        for line in yaml.dump(data, allow_unicode=True).split("\n")
        if line
    ]
    return "\n".join(lines)


def parse_user_args():
    parser = argparse.ArgumentParser(
        description="Extracts edited text fragments from Wikipedia revisions.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "input",
        default="<STDIN>",
        nargs="?",
        help="Wiki XML dump with complete edit history",
    )
    parser.add_argument(
        "output", default="<STDOUT>", nargs="?", help="File for extracted editions"
    )

    parser.add_argument(
        "-m",
        "--meta-data",
        action="store_true",
        help="add revision meta data like comment, user, etc.",
    )
    parser.add_argument(
        "-t",
        "--tabify",
        action="store_true",
        help="print output in OLD_EDIT-TAB-NEW_EDIT format",
    )
    parser.add_argument("-c", "--context", action="store_true", help="include context")
    parser.add_argument(
        "-s",
        "--scores",
        action="store_true",
        help="add levenshtein-based scores; require --tabify",
    )
    parser.add_argument("--debug", action="store_true", help="turn on debug mode")

    group = parser.add_argument_group("selection options")
    group.add_argument(
        "-l",
        "--language",
        default="english",
        help="specify language of NLTK sentence splitter",
        choices=LANGUAGES,
    )
    group.add_argument(
        "--min-chars",
        type=int,
        default=10,
        help="set the minimum number of characters in a " "sentence",
    )
    group.add_argument(
        "--min-words",
        type=int,
        default=2,
        help="set minimum length of sentence in words",
    )
    group.add_argument(
        "--max-words",
        type=int,
        default=120,
        help="set maximum length of sentence in words",
    )
    group.add_argument(
        "--length-diff",
        type=int,
        default=4,
        help="set maximum difference in length between " "edited sentences",
    )
    group.add_argument(
        "--edit-ratio",
        type=float,
        default=0.3,
        help="set maximum relative difference in edit " "distance",
    )

    args = parser.parse_args()
    if args.input == "<STDIN>":
        args.input = sys.stdin
    if args.output == "<STDOUT>":
        args.output = sys.stdout
    return args


def set_logging_level(log_level):
    if log_level is not None:
        numeric_level = getattr(logging, log_level.upper(), None)
        logging.basicConfig(level=numeric_level)


if __name__ == "__main__":
    main()

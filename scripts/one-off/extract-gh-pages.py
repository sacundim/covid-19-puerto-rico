#!/usr/bin/env python3

import argparse
import datetime
import git
import io
import logging
import pathlib

def process_arguments():
    parser = argparse.ArgumentParser(description='Extract historical contents of `gh-pages` branch')
    parser.add_argument('--repo-path', type=str, required=True,
                        help='Path to the Git repo')
    parser.add_argument('--gh-pages-branch', type=str, default='gh-pages',
                        help='Override the name of the branch')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    return parser.parse_args()

def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    args = process_arguments()
    repo = git.Repo(args.repo_path)
    for commit in reversed(list(repo.iter_commits(args.gh_pages_branch))):
        process_commit(commit, args.output_dir)

def process_commit(commit: git.Commit, output_dir: pathlib.Path):
    when = datetime.datetime.utcfromtimestamp(commit.committed_date)
    logging.info("Processing commit %s from %s", commit.hexsha, when.isoformat())
    process_tree(commit.tree, pathlib.Path(output_dir))


def process_tree(tree: git.Tree, output_dir: pathlib.Path):
    current_dir = output_dir.joinpath(tree.path)
    logging.info("Visiting %s", current_dir)
    current_dir.mkdir(exist_ok=True)
    for blob in tree.blobs:
        path = output_dir.joinpath(blob.path)
        logging.info("Writing %s", path)
        with path.open(mode='wb') as out:
            blob.stream_data(out)
    for subtree in tree.trees:
        process_tree(subtree, output_dir)

if __name__ == "__main__":
    main()
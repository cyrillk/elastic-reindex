#!/usr/bin/env python

from argparse import ArgumentParser

from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

host = 'localhost'
source = 'hotels_v4'
target = 'hotels_v5'


def parse_args():
    parser = ArgumentParser(description="ES reindex")
    parser.add_argument('-a', '--apply', action='store_true',
                        help="apply reindex")
    return parser.parse_args()


def print_count(msg, count):
    print('*** ' * 3 + msg + ' ***' * 3)
    print(count)
    print('')


def main():
    args = parse_args()
    should_apply = args.apply
    print(should_apply)

    es = Elasticsearch([{'host': host}])

    print_count("Source [before]", es.count(index=source))
    print_count("Target [before]", es.count(index=target))

    if (args.apply):
        reindex(es, source, target, chunk_size=5000, scroll='30m')

    print_count("Source [after]", es.count(index=source))
    print_count("Target [after]", es.count(index=target))


if __name__ == "__main__":
    main()

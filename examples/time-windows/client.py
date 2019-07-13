#!/usr/bin/python

import argparse
import sys

class Client(object):
    @staticmethod
    def add_args(parser):
        pass

class Source(object):
    @staticmethod
    def add_args(parser):
        pass

class Sink(object):
    @staticmethod
    def add_args(parser):
        pass

def build_parser():
    parser = argparse.ArgumentParser()
    Client.add_args(parser)
    subs = parser.add_subparsers()
    Source.add_args(subs.add_parser("source"))
    Sink.add_args(subs.add_parser("sink"))
    return parser

def run_from_args(args):
    options = build_parser().parse_args(args)
    return options.target(options)

if __name__ == '__main__':
    run_from_args(sys.argv[1:])


'''
import argparse
import sys

class CopteroArgumentsParser:
    s3confPath = ""
    s3filePath = ""


def parse(argv):

    # Construct the argument parser
    ap = argparse.ArgumentParser()

    # Add the arguments to the parser
    ap.add_argument("-c", "--conf", required=True,
                    help="s3 conf file path")
    ap.add_argument("-f", "--file", required=True,
                    help="s3 ingest file path")
    args = vars(ap.parse_args())

    s3confPath = str(args['conf'])
    s3filePath = str(args['file'])
'''
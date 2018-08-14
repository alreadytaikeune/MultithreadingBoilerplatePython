#! /usr/bin/python
from __future__ import unicode_literals, print_function, absolute_import
import sys
from math import ceil
import hashlib


def sha_hash(content, prefix=None):
    h = hashlib.new("sha256")
    if prefix:
        h.update(prefix)
    h.update(content)
    return h.hexdigest()


def solve_hashcash(seed, difficulty=0):
    bits = difficulty
    if bits % 4 != 0:
        raise ValueError("The difficulty must be a multiple of 4")
    counter = 0
    hex_digits = int(ceil(bits/4.))
    zeros = '0'*hex_digits
    while 1:
        digest = sha_hash("{0}:{1}".format(seed, hex(counter)[2:]))
        if digest[:hex_digits] == zeros:
            return hex(counter)[2:]
        counter += 1


if __name__ == "__main__":
    seed = sys.argv[1]
    difficulty = int(sys.argv[2])
    print(solve_hashcash(seed, difficulty))

#!/bin/sh
set -e

pip3 install pyflakes

pyflakes ex_py_commons

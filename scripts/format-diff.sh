#!/bin/bash

REPO_ROOT="$(git rev-parse --show-toplevel)"

if [ "$CLANG_FORMAT_DIFF" ]; then
  echo "Note: CLANG_FORMAT_DIFF='$CLANG_FORMAT_DIFF'"
  # Dry run to confirm dependencies like argparse
  if $CLANG_FORMAT_DIFF --help >/dev/null < /dev/null; then
    true #Good
  else
    exit 128
  fi
else
  # First try directly executing the possibilities
  if clang-format-diff --help &> /dev/null < /dev/null; then
    CLANG_FORMAT_DIFF=clang-format-diff
  elif clang-format-diff.py --help &> /dev/null < /dev/null; then
    CLANG_FORMAT_DIFF=clang-format-diff.py
  elif $REPO_ROOT/clang-format-diff.py --help &> /dev/null < /dev/null; then
    CLANG_FORMAT_DIFF=$REPO_ROOT/clang-format-diff.py
  else
    # This probably means we need to directly invoke the interpreter.
    # But first find clang-format-diff.py
    if [ -f "$REPO_ROOT/clang-format-diff.py" ]; then
      CFD_PATH="$REPO_ROOT/clang-format-diff.py"
    elif which clang-format-diff.py &> /dev/null; then
      CFD_PATH="$(which clang-format-diff.py)"
    else
      echo "You didn't have clang-format-diff.py and/or clang-format available in your computer!"
      echo "You can download clang-format-diff.py by running: "
      echo "    curl --location https://raw.githubusercontent.com/llvm/llvm-project/main/clang/tools/clang-format/clang-format-diff.py -o ${REPO_ROOT}/clang-format-diff.py"
      echo "You should make sure the downloaded script is not compromised."
      echo "You can download clang-format by running:"
      echo "    brew install clang-format"
      echo "  Or"
      echo "    apt install clang-format"
      echo "  This might work too:"
      echo "    yum install git-clang-format"
      echo "Then make sure clang-format is available and executable from \$PATH:"
      echo "    clang-format --version"
      exit 128
    fi
    # Check argparse pre-req on interpreter, or it will fail
    if echo import argparse | ${PYTHON:-python3}; then
      true # Good
    else
      echo "To run clang-format-diff.py, we'll need the library "argparse" to be"
      echo "installed. You can try either of the follow ways to install it:"
      echo "  1. Manually download argparse: https://pypi.python.org/pypi/argparse"
      echo "  2. easy_install argparse (if you have easy_install)"
      echo "  3. pip install argparse (if you have pip)"
      exit 129
    fi
    # Unfortunately, some machines have a Python2 clang-format-diff.py
    # installed but only a Python3 interpreter installed. Unfortunately,
    # automatic 2to3 migration is insufficient, so suggest downloading latest.
    if grep -q "print '" "$CFD_PATH" && \
       ${PYTHON:-python3} --version | grep -q 'ython 3'; then
      echo "You have clang-format-diff.py for Python 2 but are using a Python 3"
      echo "interpreter (${PYTHON:-python3})."
      echo "You can download clang-format-diff.py for Python 3 by running: "
      echo "    curl --location https://raw.githubusercontent.com/llvm/llvm-project/main/clang/tools/clang-format/clang-format-diff.py -o ${REPO_ROOT}/clang-format-diff.py"
      echo "You should make sure the downloaded script is not compromised."
      exit 130
    fi
    CLANG_FORMAT_DIFF="${PYTHON:-python3} $CFD_PATH"
    # This had better work after all those checks
    if $CLANG_FORMAT_DIFF --help >/dev/null < /dev/null; then
      true #Good
    else
      exit 128
    fi
  fi
fi

git diff `git merge-base master HEAD` ./librocksdb_sys/crocksdb | $CLANG_FORMAT_DIFF -style=google -p1 -i

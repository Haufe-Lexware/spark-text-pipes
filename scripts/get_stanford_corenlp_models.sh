#!/usr/bin/env bash

# always get the latest models from
# https://github.com/stanfordnlp/CoreNLP

MODELS="""
http://nlp.stanford.edu/software/stanford-corenlp-models-current.jar
http://nlp.stanford.edu/software/stanford-english-corenlp-models-current.jar
http://nlp.stanford.edu/software/stanford-english-kbp-corenlp-models-current.jar
http://nlp.stanford.edu/software/stanford-german-corenlp-models-current.jar
http://nlp.stanford.edu/software/stanford-french-corenlp-models-current.jar
https://github.com/stanfordnlp/CoreNLP/raw/master/lib/ejml-0.23.jar
"""

mkdir ../apps/corenlp/
cd ../apps/corenlp/

for model in ${MODELS}; do
    curl -L -s -C - -O ${model} &
done
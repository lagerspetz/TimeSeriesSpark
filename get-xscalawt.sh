#!/bin/bash
if [ ! -d XScalaWT ]; then
  git clone https://github.com/pieceoftheloaf/XScalaWT.git
else
  cd XScalaWT
  git pull
fi


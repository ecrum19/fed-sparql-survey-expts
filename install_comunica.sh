#!/usr/bin/env bash

echo "Installing comunica..."

git clone https://github.com/comunica/comunica.git
cd comunica
git checkout v4.3.0
yarn install

echo "✅ Comunica 4.3.0 successfully installed in ./comunica"
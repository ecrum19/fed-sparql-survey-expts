#!/usr/bin/env bash

echo "Installing comunica..."

git clone https://github.com/comunica/comunica.git
cd comunica
git checkout v4.4.1
yarn install

echo "✅ Comunica 4.4.1 successfully installed in ./comunica"
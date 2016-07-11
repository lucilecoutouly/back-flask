#!/bin/bash
#Change les permissions d'accès avant de lancer le programme build.sh

set -e # fail on any error

echo '* Working around permission errors locally by making sure that "www-data" uses the same uid and gid as the host volume'

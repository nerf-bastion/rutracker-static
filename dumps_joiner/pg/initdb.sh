#!/bin/bash
DB="`pwd`/`dirname "$0"`/../db"
mkdir -p "$DB"
initdb --locale=ru_RU.UTF-8 -D "$DB"

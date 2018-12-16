#!/bin/env bash

SCRIPT_DIR=`dirname "${BASH_SOURCE[0]}"`
BASE_DIR="$SCRIPT_DIR/.."

DB_PATH="$BASE_DIR/rutracker_.db"
#DB_PATH="127.0.0.1:5433"
SRC_DIR="$BASE_DIR/.."
PARSERS_DIR="$SCRIPT_DIR/parsers"

echo "final"
ruby "$PARSERS_DIR/rutracker_final.txt.rb" "$DB_PATH" "$SRC_DIR/rutracker.org_db/release/"

echo "csv"
ruby "$PARSERS_DIR/rutracker_categories.csv.rb" "$DB_PATH" "$SRC_DIR/rutracker.org_db2/rutracker-torrents"

echo "xml 2016-10"
7z e -so -mmt4 "$SRC_DIR/rutracker.org_db_xml/backup.20161015122203.7z" | tail -n+2 | cat <(echo "<torrents>") <(cat -) | ruby "$PARSERS_DIR/rutracker.rb" "$DB_PATH" 20161015.xml

echo "xml 2016-12"
unzip -p "$SRC_DIR/rutracker.org_db_xml_upd1/backup.20161212182126.zip" | tail -n+2 | cat <(echo "<torrents>") <(cat -) | ruby "$PARSERS_DIR/rutracker.rb" "$DB_PATH" 20161212.xml

echo "xml 2017-02"
unzip -p "$SRC_DIR/rutracker.org_db_xml_upd2/backup.20170208185701.zip" | ruby "$PARSERS_DIR/rutracker.rb" "$DB_PATH" 20170208.xml

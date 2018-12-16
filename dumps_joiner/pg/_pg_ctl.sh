#!/bin/bash
DB="`pwd`/`dirname "$0"`/../db"
pg_ctl -D "$DB" -l "$DB/logfile" -o "-p 5433 -k $DB" $@

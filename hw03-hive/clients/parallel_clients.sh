#!/usr/bin/env bash
# Запуск нескольких клиентов Hive одновременно

URL="${URL:-jdbc:hive2://team-37-en:10000/default}"
CLIENTS="${CLIENTS:-2}"
DIR="${DIR:-/tmp/hive-clients}"
SQL="SELECT upper(split(author, ' ')[1]) AS surname, title, length(title) AS title_len FROM books ORDER BY title_len DESC;"

mkdir -p "$DIR"
rm -f "$DIR"/c*

client() {
  date +%s > "$DIR/c$1.start"
  beeline -u "$URL" -n "client$1" --silent=true -e "$SQL" > "$DIR/c$1.log" 2>&1
  echo $? > "$DIR/c$1.rc"
  date +%s > "$DIR/c$1.end"
}

for i in $(seq 1 "$CLIENTS"); do
  client "$i" &
done
wait

ok=1
for f in "$DIR"/c*.rc; do
  c=$(basename "$f" .rc)
  echo "== $c: код $(cat "$f"), $(date -d @"$(cat "$DIR/$c.start")" +%T) - $(date -d @"$(cat "$DIR/$c.end")" +%T)"
  cat "$DIR/$c.log"
done

# если хоть у одного клиента код не 0 - ошибка
grep -qv '^0$' "$DIR"/c*.rc && ok=0

last_start=$(sort -n "$DIR"/c*.start | tail -1)
first_end=$(sort -n "$DIR"/c*.end | head -1)

[ "$last_start" -gt "$first_end" ] && ok=0

if [ "$ok" -eq 1 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
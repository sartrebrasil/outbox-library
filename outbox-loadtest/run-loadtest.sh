#!/bin/sh
# Roda o load test completo e imprime o relatório no console.
# Uso: outbox-loadtest/run-loadtest.sh [profile1,profile2,...]   (sem argumento: todos os profiles)
# Requer Docker (Testcontainers) e mvn no PATH.
set -u
cd "$(dirname "$0")/.."  # raiz do reator

ARGS=""
[ $# -gt 0 ] && ARGS="-Dloadtest.profiles=$1"

START=$(mktemp)
mvn -pl outbox-loadtest -am verify -Ploadtest $ARGS
STATUS=$?

# último relatório, só se for desta execução (nome = timestamp ISO, ordena lexicograficamente)
DIR=$(ls -d outbox-loadtest/target/loadtest/*/ 2>/dev/null | sort | tail -1)
if [ -z "$DIR" ] || [ ! "${DIR}summary.csv" -nt "$START" ]; then
  echo "Nenhum relatório gerado (build falhou antes do load test?)" >&2
  exit "${STATUS}"
fi

echo
echo "================ RELATÓRIO LOAD TEST ================"
echo "Diretório: ${DIR}  (report.json tem o detalhe completo)"
echo
column -s, -t "${DIR}summary.csv"
echo
if [ "$STATUS" -eq 0 ]; then echo "RESULTADO: OK (todos os profiles drenaram)"; else echo "RESULTADO: FALHA (algum profile não drenou ou o build quebrou)"; fi
exit "$STATUS"

#!/usr/bin/env bash
# Bring up the 5-node cluster, write a key, kill the leader, show writes continue,
# then restart the compose project and show the key survived.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/docker-compose.yml")
ADDRS="http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085"
CTL=(go run "$ROOT/cmd/raftkvctl" -addrs "$ADDRS")
KEY="demo-$(date +%s)"
VALUE="hello-raft"

ctl() {
	(cd "$ROOT" && "${CTL[@]}" "$@")
}

status_json() {
	curl -sf "http://127.0.0.1:$1/status" || true
}

leader_id() {
	local port json role
	for port in 8081 8082 8083 8084 8085; do
		json="$(status_json "$port")"
		[[ -n "$json" ]] || continue
		role="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("role",""))' <<<"$json" 2>/dev/null || true)"
		if [[ "$role" == "Leader" ]]; then
			python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$json"
			return 0
		fi
	done
	return 1
}

wait_leader() {
	local deadline=$((SECONDS + 20))
	while (( SECONDS < deadline )); do
		if leader_id >/dev/null; then
			return 0
		fi
		sleep 0.2
	done
	echo "no leader elected within 20s" >&2
	"${COMPOSE[@]}" ps >&2 || true
	return 1
}

echo "==> compose up"
"${COMPOSE[@]}" up -d --build --wait

echo "==> wait for leader"
wait_leader
LID="$(leader_id)"
echo "    leader=node${LID}"

echo "==> write ${KEY}=${VALUE} (via node that is not the leader, if possible)"
ctl put "$KEY" "$VALUE"
echo "==> read from another mapped port"
got="$(ctl get "$KEY")"
if [[ "$got" != "$VALUE" ]]; then
	echo "read mismatch: got ${got@Q} want ${VALUE@Q}" >&2
	exit 1
fi
echo "    ok"

echo "==> docker kill node${LID}"
start_ns="$(date +%s%N)"
"${COMPOSE[@]}" kill "node${LID}"
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
	if ctl put "${KEY}-after" "survived" >/dev/null 2>&1; then
		end_ns="$(date +%s%N)"
		break
	fi
	sleep 0.05
done
if [[ -z "${end_ns:-}" ]]; then
	echo "writes did not resume within 30s after killing the leader" >&2
	exit 1
fi
failover_ms="$(( (end_ns - start_ns) / 1000000 ))"
echo "    first successful write after kill: ${failover_ms}ms"
echo "DEMO_FAILOVER_MS=${failover_ms}"

echo "==> compose down && up (volumes retained)"
"${COMPOSE[@]}" down
"${COMPOSE[@]}" up -d --wait
wait_leader
got="$(ctl get "$KEY")"
if [[ "$got" != "$VALUE" ]]; then
	echo "data lost across compose restart: got ${got@Q}" >&2
	exit 1
fi
got="$(ctl get "${KEY}-after")"
if [[ "$got" != "survived" ]]; then
	echo "post-failover write lost across restart: got ${got@Q}" >&2
	exit 1
fi
echo "    both keys present after restart"

echo
echo "cluster is still up. HTTP ports 8081-8085. Tear down with:"
echo "  docker compose -f deploy/docker-compose.yml down"

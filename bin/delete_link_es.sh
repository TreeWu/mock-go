#!/usr/bin/env bash

# Delete link resources and related relations from Elasticsearch.
# Configure the variables below before running. Keep DRY_RUN=true for the first check.
#
# Example:
#   bash delete_link_es.sh
#
# Notes:
# - Resource deletion defaults to id.keyword because this ES index stores the
#   business id in the id field.
# - Prefix deletion needs normal searchable keyword fields. Relation _id in this
#   project is resource_id1->resource_id2, and this ES index can query the same
#   business id from id.keyword.

set -euo pipefail

# Elasticsearch connection. If ES_USER/ES_PASS are empty, wget will not use auth.
ES_URL="http://127.0.0.1:9200"
ES_USER=""
ES_PASS=""

RESOURCES_INDEX="gj_xcmdb_resources"
RELATIONS_INDEX="gj_xcmdb_relations"

# Use .keyword for text fields. If id/resource_id1/resource_id2 are already
# keyword fields, change these to "id", "resource_id1", and "resource_id2".
RESOURCE_PREFIX_FIELD="id.keyword"
RELATION_PREFIX_FIELD="id.keyword"

RESOURCE_EXACT_FIELD="id.keyword"
RESOURCE_ID1_FIELD="resource_id1.keyword"
RESOURCE_ID2_FIELD="resource_id2.keyword"

DRY_RUN="true"
REFRESH="true"

# Change this to the subsystem/link ids that need to be deleted.
LINK_IDS=(
  "replace_with_link_id"
)

if [[ "${#LINK_IDS[@]}" -eq 0 || "${LINK_IDS[0]}" == "replace_with_link_id" ]]; then
  echo 'Please edit LINK_IDS before running this script.'
  exit 1
fi

WGET_AUTH_ARGS=()
if [[ -n "$ES_USER" ]]; then
  WGET_AUTH_ARGS+=(--user="$ES_USER")
  if [[ -n "$ES_PASS" ]]; then
    WGET_AUTH_ARGS+=(--password="$ES_PASS")
  fi
fi

es_request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"

  if [[ -n "$body" ]]; then
    wget -qO- \
      --header='Content-Type: application/json' \
      --method="$method" \
      --body-data="$body" \
      "${WGET_AUTH_ARGS[@]}" \
      "$ES_URL$path"
  else
    wget -qO- \
      --header='Content-Type: application/json' \
      --method="$method" \
      "${WGET_AUTH_ARGS[@]}" \
      "$ES_URL$path"
  fi
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  printf '%s' "$value"
}

exists_resource() {
  local id
  id="$(json_escape "$1")"

  local body
  body='{"query":{"term":{"'"$RESOURCE_EXACT_FIELD"'":"'"$id"'"}},"size":0,"track_total_hits":true}'

  es_request POST "/$RESOURCES_INDEX/_search" "$body" |
    grep -Eq '"total"[[:space:]]*:[[:space:]]*\{[[:space:]]*"value"[[:space:]]*:[[:space:]]*[1-9]'
}

run_delete_by_query() {
  local index="$1"
  local body="$2"
  local path

  if [[ "$DRY_RUN" == "true" ]]; then
    path="/$index/_count"
    es_request POST "$path" "$body"
  else
    path="/$index/_delete_by_query?conflicts=proceed&refresh=$REFRESH"
    es_request POST "$path" "$body"
  fi
}

for raw_link_id in "${LINK_IDS[@]}"; do
  link_id="$(json_escape "$raw_link_id")"

  if ! exists_resource "$raw_link_id"; then
    echo "skip link, resource not found: $raw_link_id"
    continue
  fi

  echo "delete link: $raw_link_id"

  # Mongo equivalent:
  # db.resources.deleteMany({ _id: { $regex: "^" + link._id + "_.*" } })
  run_delete_by_query "$RESOURCES_INDEX" \
    '{"query":{"prefix":{"'"$RESOURCE_PREFIX_FIELD"'":"'"$link_id"'_"}}}'
  echo

  # Mongo equivalent:
  # db.relations.deleteMany({ _id: { $regex: "^" + link._id + "_.*" } })
  run_delete_by_query "$RELATIONS_INDEX" \
    '{"query":{"prefix":{"'"$RELATION_PREFIX_FIELD"'":"'"$link_id"'_"}}}'
  echo

  # Mongo equivalent:
  # db.relations.deleteMany({ resource_id1: link._id })
  run_delete_by_query "$RELATIONS_INDEX" \
    '{"query":{"term":{"'"$RESOURCE_ID1_FIELD"'":"'"$link_id"'"}}}'
  echo

  # Mongo equivalent:
  # db.relations.deleteMany({ resource_id2: link._id })
  run_delete_by_query "$RELATIONS_INDEX" \
    '{"query":{"term":{"'"$RESOURCE_ID2_FIELD"'":"'"$link_id"'"}}}'
  echo

  # Mongo equivalent:
  # db.resources.deleteOne({ _id: link._id })
  run_delete_by_query "$RESOURCES_INDEX" \
    '{"query":{"term":{"'"$RESOURCE_EXACT_FIELD"'":"'"$link_id"'"}}}'
  echo
done

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY_RUN=true, only counted matched documents. Set DRY_RUN=false to delete."
else
  echo "delete finished."
fi

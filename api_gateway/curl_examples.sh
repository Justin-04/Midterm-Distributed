#!/usr/bin/env bash
# ============================================================
# curl_examples.sh — API Gateway example requests
# Set BASE_URL to your gateway address before running.
# ============================================================

BASE_URL="http://localhost:9100"

echo "=== 1. Get JWT token (admin) ==="
TOKEN=$(curl -s -X POST "$BASE_URL/auth/token" \
  -d "username=admin&password=adminpass" | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "Token: ${TOKEN:0:40}..."

AUTH="Authorization: Bearer $TOKEN"

echo ""
echo "=== 2. Create a bucket ==="
curl -s -X PUT "$BASE_URL/buckets/my-bucket" \
  -H "$AUTH" \
  -H "Content-Type: application/json" \
  -d '{"encoding":"replication","replication_factor":3,"write_quorum":2,"read_quorum":2}' | python -m json.tool

echo ""
echo "=== 3. Upload a file ==="
curl -s -X PUT "$BASE_URL/buckets/my-bucket/hello.txt" \
  -H "$AUTH" \
  -H "Content-Type: text/plain" \
  -H "Tags: project=demo,env=dev" \
  --data-binary "Hello, distributed storage!" | python -m json.tool

echo ""
echo "=== 4. Upload via multipart ==="
curl -s -X PUT "$BASE_URL/buckets/my-bucket/file.bin" \
  -H "$AUTH" \
  -F "file=@/etc/hostname;type=application/octet-stream" | python -m json.tool

echo ""
echo "=== 5. Download full file ==="
curl -s -X GET "$BASE_URL/buckets/my-bucket/hello.txt" \
  -H "$AUTH"

echo ""
echo "=== 6. Byte-range download (first 10 bytes) ==="
curl -s -X GET "$BASE_URL/buckets/my-bucket/hello.txt" \
  -H "$AUTH" \
  -H "Range: bytes=0-9" -D -

echo ""
echo "=== 7. List objects ==="
curl -s "$BASE_URL/buckets/my-bucket/objects" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 8. Get object metadata ==="
curl -s "$BASE_URL/buckets/my-bucket/objects/hello.txt" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 9. Update tags ==="
curl -s -X PATCH "$BASE_URL/buckets/my-bucket/objects/hello.txt" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"tags":{"env":"prod","team":"storage"}}' | python -m json.tool

echo ""
echo "=== 10. Search by prefix ==="
curl -s "$BASE_URL/search?bucket=my-bucket&prefix=hel" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 11. Search by size range ==="
curl -s "$BASE_URL/search?min_size=1&max_size=1000" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 12. Search by tag ==="
curl -s "$BASE_URL/search?project=demo" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 13. Cluster status ==="
curl -s "$BASE_URL/cluster/status" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 14. Ring distribution ==="
curl -s "$BASE_URL/ring/stats" -H "$AUTH" | python -m json.tool

echo ""
echo "=== 15. Delete object ==="
curl -s -X DELETE "$BASE_URL/buckets/my-bucket/hello.txt" \
  -H "$AUTH" -w "HTTP %{http_code}\n"

echo ""
echo "=== 16. Delete bucket ==="
curl -s -X DELETE "$BASE_URL/buckets/my-bucket" \
  -H "$AUTH" -w "HTTP %{http_code}\n"

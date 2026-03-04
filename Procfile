web: uvicorn sarc.api.main:app --host 0.0.0.0 --port $PORT
fetch_hourly: sh /workspace/scripts/gcp/fetch_hourly.sh
parse_hourly: sh /workspace/scripts/gcp/parse_hourly.sh
fetch_daily: sh /workspace/scripts/gcp/fetch_daily.sh
parse_daily: sh /workspace/scripts/gcp/parse_daily.sh
fetch_manual: sh /workspace/scripts/gcp/fetch_manula.sh
parse_manual: sh /workspace/scripts/gcp/parse_manual.sh
health_check: sarc health run --all

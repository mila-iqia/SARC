web: uvicorn sarc.api.main:app --host 0.0.0.0 --port $PORT
# We will finalize those commands later
fetch_hourly: sarc fetch
parse_hourly: sarc parse
fetch_daily: sarc fetch
parse_daily: sarc parse
fetch_manual: sarc fetch
parse_manual: sarc parse
parse_daily: sarc health run

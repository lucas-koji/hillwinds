set -e

echo "Running SQL scripts..."

echo "  -> gaps.sql"
duckdb -csv < sql/gaps.sql > outputs/sql_gaps.csv

echo "  -> spikes.sql"
duckdb -csv < sql/spikes.sql > outputs/sql_spikes.csv

echo "  -> roster.sql"
duckdb -csv < sql/roster.sql > outputs/sql_roster.csv

echo "Outputs saved to outputs/"

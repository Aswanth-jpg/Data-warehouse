import duckdb
import pandas as pd

# Connect in-memory
con = duckdb.connect()

# Register CSVs as views
con.execute("CREATE VIEW fact_orders AS SELECT * FROM read_csv_auto('fact_orders.csv')")
con.execute("CREATE VIEW dim_customers AS SELECT * FROM read_csv_auto('dim_customers.csv')")
con.execute("CREATE VIEW dim_products AS SELECT * FROM read_csv_auto('dim_products.csv')")
con.execute("CREATE VIEW dim_date AS SELECT * FROM read_csv_auto('dim_date.csv')")

# Parse queries.sql into (heading, query) pairs.
# Headings are taken from SQL comment lines that immediately precede a statement (lines starting with --).
statements = []
with open("queries.sql", "r") as f:
    current_lines = []
    comment_lines = []
    for raw in f:
        line = raw.rstrip("\n")
        stripped = line.strip()
        if stripped.startswith("--"):
            # collect comment text without leading dashes
            comment_lines.append(stripped.lstrip('- ').strip())
            continue
        # accumulate SQL lines
        if not line and not current_lines:
            # skip blank lines
            continue
        current_lines.append(line)
        if ';' in line:
            q = '\n'.join(current_lines).strip()
            if q.endswith(';'):
                q = q[:-1].strip()
            heading = ' '.join(comment_lines).strip() if comment_lines else None
            statements.append((heading, q))
            current_lines = []
            comment_lines = []
    # handle any trailing statement without a semicolon
    if current_lines:
        q = '\n'.join(current_lines).strip()
        heading = ' '.join(comment_lines).strip() if comment_lines else None
        statements.append((heading, q))

# Execute each statement and print the heading followed by result values only
for idx, (heading, query) in enumerate(statements, start=1):
    try:
        df = con.execute(query).df()
        # Print heading as separator; if no heading, use a generic one
        if heading:
            print(heading)
        else:
            print(f"Query {idx}")

        if df.empty:
            # keep output minimal: no rows to print for this query
            continue

        # Print only query result values without the DataFrame index
        print(df.to_string(index=False))
    except Exception as e:
        print("Error:", e)

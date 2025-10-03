import psycopg2
import configparser
import csv
import sqlparse
import traceback
import os

print("Current directory:", os.getcwd())
print("Files here:", os.listdir())
print("Does feature_config.ini exist?", os.path.exists("feature_config.ini"))

# -------------------------
# Helper: Extract table names from query
# -------------------------
def get_table_names(query):
    parsed = sqlparse.parse(query)
    tables = []
    from_seen = False

    for statement in parsed:
        for token in statement.tokens:
            if from_seen:
                if isinstance(token, sqlparse.sql.Identifier):
                    table_name = token.get_real_name()
                    if table_name:
                        tables.append(table_name)
                elif isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        table_name = identifier.get_real_name()
                        if table_name:
                            tables.append(table_name)
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ("FROM", "JOIN"):
                from_seen = True

    return tables


# -------------------------
# Helper: Extract features from query plan
# -------------------------
def extract_features(plan, query_index, query, cur, level=1, parent=None, node_num=1):
    # Extract loop
    loop = plan.get('Actual Loops', 1)

    # Extract input cardinality
    if 'Plans' in plan:
        input_cardinality = sum(child.get('Actual Rows', 0) for child in plan.get('Plans', []))
    else:
        relation_name = plan.get("Relation Name")
        if relation_name:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{relation_name}"')
                input_cardinality = cur.fetchone()[0]
            except Exception:
                input_cardinality = -1
        else:
            input_cardinality = -1

    # Extract output cardinality
    output_cardinality = plan.get('Actual Rows')

    # Extract base cardinality
    base_cardinality = 0
    tables = get_table_names(query)
    for table in tables:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cur.fetchone()[0]
            base_cardinality += count
        except Exception:
            base_cardinality += -1

    # Add level feature
    level_feature = f'{level}.{node_num}'
    feature_parent = '-1' if parent is None else parent

    features = {
        'query_index': query_index,
        'query': query if level == 1 else '',
        'node_type': plan.get('Node Type', ''),
        'parallel_aware': plan.get('Parallel Aware', ''),
        'startup_cost (ms)': plan.get('Startup Cost', ''),
        'total_cost (ms)': plan.get('Total Cost', ''),
        'plan_rows': plan.get('Plan Rows', ''),
        'plan_width': plan.get('Plan Width', ''),
        'actual_startup_time (ms)': plan.get('Actual Startup Time', ''),
        'actual_total_time (ms)': plan.get('Actual Total Time', ''),
        'input_cardinality (rows)': input_cardinality,
        'output_cardinality (rows)': output_cardinality,
        'base_cardinality (rows)': base_cardinality,
        'loop': loop,
        'level_no': level_feature,
        'parent_level': feature_parent
    }

    yield features

    if 'Plans' in plan:
        for i, subplan in enumerate(plan['Plans']):
            yield from extract_features(subplan, query_index, query, cur, level=level+1,
                                        parent=level_feature, node_num=i+1)


# -------------------------
# Main Script
# -------------------------

# Read configuration
config = configparser.ConfigParser()
config.read('feature_config.ini')

# Establish DB connection
db_params = config['postgres']
conn = psycopg2.connect(
    host=db_params.get('host', '10.100.71.21'),
    database=db_params.get('database', '202418018'),
    user=db_params.get('user', '202418018'),
    password=db_params.get('password', '202418018'),
    port=db_params.get('port', '5432')
)

# File paths
file_params = config['files']
queries_file = file_params['imdb_queries']
result_file = file_params['csv_file']
error_log = "query_errors.log"

with open(result_file, 'w', newline='', encoding='utf-8') as result_csv_file, \
     open(error_log, 'w', encoding='utf-8') as error_file:

    fieldnames = [
        'query_index', 'query', 'level_no', 'parent_level',
        'node_type', 'parallel_aware',
        'startup_cost (ms)', 'total_cost (ms)',
        'plan_rows', 'plan_width',
        'actual_startup_time (ms)', 'actual_total_time (ms)',
        'input_cardinality (rows)', 'output_cardinality (rows)',
        'base_cardinality (rows)', 'loop'
    ]
    csv_writer = csv.DictWriter(result_csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()

    with open(queries_file, 'r', encoding='utf-8') as f:
        queries = f.read().split(';')
        queries = [q.strip() for q in queries if q.strip()]

    print(f"✅ Loaded {len(queries)} queries from {queries_file}")

    for query_index, query in enumerate(queries, start=1):
        cur = conn.cursor()
        try:
            print(f"\n▶ Running query {query_index}: {query[:80]}...")
            cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
            result = cur.fetchone()[0]

            if isinstance(result, list) and result:
                result = result[0]

            plan = result['Plan']

            for features in extract_features(plan, query_index, query, cur):
                csv_writer.writerow(features)

        except Exception as e:
            error_msg = (
                f"⚠️ Query {query_index} failed: {query}\n"
                f"Error: {str(e)}\n"
                f"Traceback:\n{traceback.format_exc()}\n{'-'*80}\n"
            )
            print(error_msg)
            error_file.write(error_msg)
            conn.rollback()
        finally:
            cur.close()

conn.close()
print(f"\n✅ Feature extraction completed.\nResults saved in {result_file}\nErrors logged in {error_log}")

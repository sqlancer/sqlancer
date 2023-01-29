import os


def get_java_files(directory):
	java_files = []
	for root, dirs, files in os.walk(directory):
		for f in files:
			if f.endswith('.java'):
				java_files.append(f)
	return java_files

def verify_prefix(prefix, files):
	if len(files) == 0:
		print(prefix + ' directory does not contain any files!')
		exit(-1)
	for f in files:
		if not f.startswith(prefix):
			print('The class name of ' + f + ' does not start with ' + prefix)
			exit(-1)

verify_prefix('ArangoDB', get_java_files("sqlancer/arangodb/"))
verify_prefix('Citus', get_java_files("sqlancer/citus/"))
verify_prefix('ClickHouse', get_java_files("sqlancer/clickhouse/"))
verify_prefix('CnosDB', get_java_files("sqlancer/cnosdb/"))
verify_prefix('CockroachDB', get_java_files("sqlancer/cockroachdb/"))
verify_prefix('Cosmos', get_java_files("sqlancer/cosmos/"))
verify_prefix('Databend', get_java_files("sqlancer/databend/"))
verify_prefix('DuckDB', get_java_files("sqlancer/duckdb"))
verify_prefix('H2', get_java_files("sqlancer/h2"))
verify_prefix('HSQLDB', get_java_files("sqlancer/hsqldb"))
verify_prefix('MariaDB', get_java_files("sqlancer/mariadb/"))
verify_prefix('MySQL', get_java_files("sqlancer/mysql/"))
verify_prefix('OceanBase', get_java_files("sqlancer/oceanbase/"))
verify_prefix('Postgres', get_java_files("sqlancer/postgres/"))
verify_prefix('QuestDB', get_java_files("sqlancer/questdb/"))
verify_prefix('SQLite3', get_java_files("sqlancer/sqlite3/"))
verify_prefix('TiDB', get_java_files("sqlancer/tidb/"))
verify_prefix('Y', get_java_files("sqlancer/yugabyte/")) # has both YCQL and YSQL prefixes

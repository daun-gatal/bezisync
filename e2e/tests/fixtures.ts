/** Addresses of the e2e data plane (deploy/e2e/fixtures). */
export const SOURCE_PG = {
  host: 'source-pg.bezisync-e2e-data.svc.cluster.local',
  port: 5432,
  database: 'shop',
  user: 'cdc_reader',
  password: 'cdc-reader-pw',
};
export const LEGACY_PG = {
  host: 'legacy-pg.bezisync-e2e-data.svc.cluster.local',
  port: 5432,
  database: 'legacy',
  user: 'app_user',
  password: 'legacy-admin-pw',
};
export const KAFKA = { bootstrap_servers: 'kafka.bezisync-e2e-data.svc.cluster.local:9092' };
/** Direct access from the runner for writing test rows. */
export const SOURCE_PG_FROM_RUNNER = {
  host: 'bezisync-e2e-k3s',
  port: 30432,
  database: 'shop',
  user: 'e2e_writer',
  password: 'e2e-writer-pw',
};
export const HTTP_ECHO = 'http://http-echo.bezisync-e2e-data.svc.cluster.local:8080';
export const HTTP_ECHO_FROM_RUNNER = 'http://bezisync-e2e-k3s:30808';
export const MYSQL = { host: 'source-mysql.bezisync-e2e-data.svc.cluster.local', port: 3306, user: 'cdc_reader', password: 'cdc-reader-pw' };
export const MYSQL_FROM_RUNNER = { host: 'bezisync-e2e-k3s', port: 30306, user: 'e2e_writer', password: 'e2e-writer-pw', database: 'shop' };
export const MSSQL = { host: 'source-mssql.bezisync-e2e-data.svc.cluster.local', port: 1433, database: 'Shop', user: 'cdc_reader', password: 'Cdc-reader-pw1!' };
export const MSSQL_FROM_RUNNER = { server: 'bezisync-e2e-k3s', port: 31433, database: 'Shop', user: 'e2e_writer', password: 'E2e-writer-pw1!' };
export const MONGO = { hosts: 'source-mongo.source-mongo-hl.bezisync-e2e-data.svc.cluster.local:27017', replica_set: 'rs0', user: 'cdc_reader', password: 'cdc-reader-pw' };
export const MONGO_FROM_RUNNER = 'mongodb://e2e_writer:e2e-writer-pw@bezisync-e2e-k3s:30017/?authSource=admin&directConnection=true';
const DATA = 'bezisync-e2e-data.svc.cluster.local';
export const REDIS = { host: `redis.${DATA}`, port: 6379, password: 'redis-pw' };
export const NATS = { url: `nats://nats.${DATA}:4222` };
export const KINESIS = { region: 'us-east-1', endpoint: `http://kinesis.${DATA}:4566`, access_key_id: 'test', secret_access_key: 'test' };
export const PUBSUB = { project_id: 'bezisync-e2e', emulator_host: `pubsub.${DATA}:8681` };
export const PUBSUB_FROM_RUNNER = 'http://bezisync-e2e-k3s:30681';
/** RustFS's built-in Iceberg catalog (S3 Tables) on the "lake" table bucket. */
export const RUSTFS_TABLES = {
  catalog_type: 'rest', uri: `http://rustfs.${DATA}:9000/iceberg`, warehouse: 'lake', namespace: 'e2e', auth: 'sigv4', signing_name: 's3',
  region: 'us-east-1', storage: 's3', s3_endpoint: `http://rustfs.${DATA}:9000`, path_style: true, access_key_id: 'lake-admin', secret_access_key: 'lake-secret-123',
};
/** Apache Iceberg's REST catalog fixture, files in RustFS's "warehouse" bucket. */
export const ICEBERG_REST = {
  catalog_type: 'rest', uri: `http://iceberg-rest.${DATA}:8181`, warehouse: 's3://warehouse/', namespace: 'e2e', auth: 'none', storage: 's3',
  region: 'us-east-1', s3_endpoint: `http://rustfs.${DATA}:9000`, path_style: true, access_key_id: 'lake-admin', secret_access_key: 'lake-secret-123',
};
export const VAULT = { address: `http://vault.${DATA}:8200`, token: 'e2e-vault-root' };
export const VAULT_FROM_RUNNER = 'http://bezisync-e2e-k3s:30820';
export const MOTO = { endpoint: `http://moto.${DATA}:5000`, region: 'us-east-1' };
export const MOTO_FROM_RUNNER = 'http://bezisync-e2e-k3s:30500';
/** SSH bastion fixture (80-bastion.yaml). Test-only key pair, generated for the e2e suite. */
export const BASTION = { ssh_host: `bastion.${DATA}`, ssh_port: 2222, ssh_user: 'jump' };
export const BASTION_KEY = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\nQyNTUxOQAAACCXAvyQN/8qZmGVxdFs0vZKXNDQzUJ43ILL9oX90DEUewAAAJg29bTBNvW0\nwQAAAAtzc2gtZWQyNTUxOQAAACCXAvyQN/8qZmGVxdFs0vZKXNDQzUJ43ILL9oX90DEUew\nAAAEDneqPSkK5ppCxIOtBf4o4VPUQn8zCWTWnr7wF/+lGdepcC/JA3/ypmYZXF0WzS9kpc\n0NDNQnjcgsv2hf3QMRR7AAAAEWJlemlzeW5jLWUyZS1vbmx5AQIDBA==\n-----END OPENSSH PRIVATE KEY-----\n";
/** JDBC destinations (deploy/e2e/fixtures/46-sql-targets.yaml). */
export const TARGET_PG = { database_type: 'postgresql', host: `target-pg.${DATA}`, port: 5432, database: 'warehouse', schema: 'public', user: 'bz_writer', tls: 'disable', password: 'bz-writer-pw' };
export const TARGET_MYSQL = { database_type: 'mysql', host: `target-mysql.${DATA}`, port: 3306, database: 'warehouse', user: 'bz_writer', tls: 'require', password: 'bz-writer-pw' };
/** Azure Event Hubs emulator (deploy/e2e/fixtures/47-eventhubs.yaml), hub "bezisync". */
export const EVENTHUBS = {
  hub_name: 'bezisync',
  connection_string: `Endpoint=sb://eventhubs.${DATA};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`,
};
/** Apache Pulsar standalone (deploy/e2e/fixtures/48-pulsar.yaml). */
export const PULSAR = { service_url: `pulsar://pulsar.${DATA}:6650`, admin_url: `http://pulsar.${DATA}:8080`, tenant: 'public', namespace: 'default' };
/** Oracle Free (15-source-oracle.yaml): LogMiner common user on the CDB, tables in the PDB. */
export const ORACLE = { host: `source-oracle.${DATA}`, port: 1521, database: 'FREE', pdb: 'FREEPDB1', user: 'c##cdc_reader', password: 'cdc-reader-pw' };
/** Oracle XE 21c (15-source-oracle-xe.yaml), for Flink CDC's Oracle reader (it can't read Oracle Free). */
export const ORACLE_XE = { host: `source-oracle-xe.${DATA}`, port: 1521, database: 'XE', pdb: 'XEPDB1', user: 'c##cdc_reader', password: 'cdc-reader-pw' };
/** CockroachDB single node, insecure (16-source-cockroachdb.yaml). */
export const COCKROACHDB = { host: `source-cockroachdb.${DATA}`, port: 26257, database: 'shop', user: 'cdc_reader', ssl_mode: 'disable' };
export const COCKROACHDB_FROM_RUNNER = { host: 'bezisync-e2e-k3s', port: 32657, database: 'shop', user: 'e2e_writer' };
/** Vitess test cluster (17-source-vitess.yaml): VTGate gRPC and MySQL ports, keyspace "shop" in two shards. */
export const VITESS = { host: `source-vitess.${DATA}`, port: 33575, mysql_port: 33577, keyspace: 'shop', tablet_type: 'replica' };
export const VITESS_FROM_RUNNER = { host: 'bezisync-e2e-k3s', port: 32306, user: 'root', database: 'shop' };
/** Cloud Spanner emulator (18-source-spanner.yaml): instance "shop", database "orders". */
export const SPANNER = { project_id: 'bezisync-e2e', instance_id: 'shop', database_id: 'orders', emulator_host: `spanner.${DATA}:9010` };
export const SPANNER_REST_FROM_RUNNER = 'http://bezisync-e2e-k3s:30920/v1/projects/bezisync-e2e/instances/shop/databases/orders';
/** Flink CDC-only destinations (49-flink-sinks.yaml) and Paimon on the lakehouse S3. */
export const ELASTICSEARCH = { hosts: `http://elasticsearch.${DATA}:9200`, version: '8', username: 'elastic', password: 'es-pw' };
export const OCEANBASE = { host: `oceanbase.${DATA}`, query_port: 2881, database: 'cdc', user: 'root@test', password: 'ob-pw' };
export const PAIMON = { warehouse: 's3://warehouse/paimon', s3_endpoint: `http://rustfs.${DATA}:9000`, region: 'us-east-1', path_style: true, access_key_id: 'lake-admin', secret_access_key: 'lake-secret-123' };
/** Opt-in StarRocks and Doris (deploy/e2e/fixtures-olap, E2E_OLAP=1): database cdc, user bz_writer. */
export const STARROCKS = { host: `starrocks.${DATA}`, query_port: 9030, http_port: 8080, database: 'cdc', user: 'bz_writer', replicas: 1, password: 'bz-writer-pw' };
export const DORIS = { host: `doris.${DATA}`, query_port: 9030, http_port: 8030, be_nodes: `doris.${DATA}:8040`, database: 'cdc', user: 'bz_writer', replicas: 1, password: 'bz-writer-pw' };

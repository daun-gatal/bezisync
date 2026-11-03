package connectors

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// MongoDB source (change streams).

const (
	mongoManagedDB      = "bezisync"
	mongoMaxCollections = 10_000
)

var mongoSystemDBs = map[string]bool{"admin": true, "local": true, "config": true, mongoManagedDB: true}

type mongoSource struct{ BaseSource }

func init() { Register(&mongoSource{BaseSource{Title: "MongoDB"}}) }

func (*mongoSource) Kind() string { return "source" }
func (*mongoSource) Type() string { return "mongodb" }

type mongoConfig struct {
	Hosts, User, AuthSource, CaptureMode string
	ReplicaSet                           *string
	TLS, SRV                             bool
}

func mongoCfg(conn Resolved) (mongoConfig, map[string]any, error) {
	d, _ := Describe("source", "mongodb")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return mongoConfig{}, nil, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return mongoConfig{}, nil, err
	}
	return mongoConfig{Hosts: Str(cfg, "hosts"), User: Str(cfg, "user"), AuthSource: Str(cfg, "auth_source"),
		CaptureMode: Str(cfg, "capture_full_documents"), ReplicaSet: OptStr(cfg, "replica_set"),
		TLS: Bool(cfg, "tls"), SRV: Bool(cfg, "srv")}, sec, nil
}

// MongoConnectionString builds the URI (quote_plus-encoded credentials).
func MongoConnectionString(cfg mongoConfig, password string) string {
	scheme := "mongodb"
	if cfg.SRV {
		scheme = "mongodb+srv"
	}
	params := []string{"authSource=" + cfg.AuthSource}
	if cfg.ReplicaSet != nil && *cfg.ReplicaSet != "" {
		params = append(params, "replicaSet="+*cfg.ReplicaSet)
	}
	if cfg.TLS {
		params = append(params, "tls=true")
	}
	return fmt.Sprintf("%s://%s:%s@%s/?%s", scheme, url.QueryEscape(cfg.User), url.QueryEscape(password), cfg.Hosts, strings.Join(params, "&"))
}

func mongoConnect(ctx context.Context, conn Resolved, timeout time.Duration) (*mongo.Client, func(), error) {
	cfg, sec, err := mongoCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	opts := options.Client().ApplyURI(MongoConnectionString(cfg, PyStr(sec["password"]))).
		SetServerSelectionTimeout(timeout).SetConnectTimeout(timeout)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, nil, err
	}
	return client, func() { _ = client.Disconnect(context.Background()) }, nil
}

func mongoSignalCollection(c CompileContext) string { return mongoManagedDB + ".signal_" + c.ShortID }

func mongoAuthFailed(err error) (int, bool) {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return int(ce.Code), true
	}
	var se mongo.ServerError
	if errors.As(err, &se) {
		for _, code := range []int{18, 13, 8000} {
			if se.HasErrorCode(code) {
				return code, true
			}
		}
		return 0, true
	}
	return 0, false
}

func (s *mongoSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, _, err := mongoCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	if !cfg.SRV {
		firstHost := strings.TrimSpace(strings.Split(cfg.Hosts, ",")[0])
		var host, port string
		if i := strings.LastIndex(firstHost, ":"); i >= 0 {
			host, port = firstHost[:i], firstHost[i+1:]
		}
		p := 27017
		if isDigits(port) {
			p, _ = strconv.Atoi(port)
		}
		if host == "" {
			host = firstHost
		}
		if !NetworkCheck(ctx, run, host, p, "mongo_network", "MongoDB", 0) {
			return checks.Summarize(run.Checks, started), nil
		}
	}
	client, done, err := mongoConnect(ctx, conn, 6*time.Second)
	if err != nil {
		s.failure(run, err)
		return checks.Summarize(run.Checks, started), nil
	}
	defer done()
	var hello bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		s.failure(run, err)
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("mongo_login", "Sign in", checks.CatAuth, fmt.Sprintf("Signed in as %s.", cfg.User), "")
	if err := s.serverChecks(ctx, client, hello, run); err != nil {
		s.failure(run, err)
		return checks.Summarize(run.Checks, started), nil
	}
	if len(tables) > 0 {
		if err := s.collectionChecks(ctx, client, run, tables); err != nil {
			s.failure(run, err)
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *mongoSource) failure(run *checks.Run, err error) {
	if code, ok := mongoAuthFailed(err); ok {
		msg := "MongoDB refused the request."
		if code == 18 {
			msg = "MongoDB rejected the credentials."
		}
		run.Add(checks.Check{Code: "mongo_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: msg, Technical: firstRunes(err.Error(), 400)})
		return
	}
	if strings.Contains(err.Error(), "AuthenticationFailed") || strings.Contains(err.Error(), "auth error") {
		run.Add(checks.Check{Code: "mongo_login", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "MongoDB rejected the credentials.", Technical: firstRunes(err.Error(), 400)})
		return
	}
	run.Add(checks.Check{Code: "mongo_connect", Title: "Connect", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
		Message: "Could not reach a usable MongoDB primary.", Technical: firstRunes(err.Error(), 400),
		Why: "For replica sets, every member's advertised hostname must resolve from the runtime."})
}

func (s *mongoSource) serverChecks(ctx context.Context, client *mongo.Client, hello bson.M, run *checks.Run) error {
	var info bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildinfo", Value: 1}}).Decode(&info); err != nil {
		return err
	}
	version := "0"
	if v, ok := info["version"]; ok {
		version = fmt.Sprint(v)
	}
	major, _ := strconv.Atoi(strings.Split(version, ".")[0])
	if major < 6 {
		run.Add(checks.Check{Code: "mongo_version", Title: "MongoDB version", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: fmt.Sprintf("MongoDB %s is too old; Debezium 3 needs 6.0 or newer.", version)})
	} else {
		run.Passed("mongo_version", "MongoDB version", checks.CatCDC, fmt.Sprintf("MongoDB %s.", version), "")
	}
	msg, _ := hello["msg"].(string)
	setName, _ := hello["setName"].(string)
	if msg == "isdbgrid" || setName != "" {
		text := fmt.Sprintf("Replica set %s.", setName)
		if msg == "isdbgrid" {
			text = "Sharded cluster."
		}
		run.Passed("mongo_topology", "Change streams available", checks.CatCDC, text, "")
	} else {
		run.Add(checks.Check{Code: "mongo_topology", Title: "Change streams available", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "This is a standalone server; change streams need a replica set.",
			Fix:     &checks.Fix{Kind: "shell", RestartRequired: true, Who: "DBA", Code: "mongod --replSet rs0 …\nmongosh --eval 'rs.initiate()'"}})
	}
	return nil
}

func listCollections(ctx context.Context, db *mongo.Database) ([]string, error) {
	return db.ListCollectionNames(ctx, bson.D{})
}

func (s *mongoSource) collectionChecks(ctx context.Context, client *mongo.Client, run *checks.Run, tables []string) error {
	var missing []string
	for _, name := range tables {
		db, coll, _ := strings.Cut(name, ".")
		names, err := listCollections(ctx, client.Database(db))
		if err != nil {
			return err
		}
		if !containsStr(names, coll) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "mongo_collections_exist", Title: "Selected collections exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
		return nil
	}
	db, coll, _ := strings.Cut(tables[0], ".")
	stream, err := client.Database(db).Collection(coll).Watch(ctx, mongo.Pipeline{}, options.ChangeStream().SetMaxAwaitTime(200*time.Millisecond))
	if err == nil {
		stream.TryNext(ctx)
		err = stream.Err()
		_ = stream.Close(ctx)
	}
	if err != nil {
		if _, ok := mongoAuthFailed(err); !ok {
			return err
		}
		run.Add(checks.Check{Code: "mongo_change_stream", Title: "Read change streams", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: "This user may not open change streams.", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "shell", Who: "DBA", Code: "db.grantRolesToUser('<user>', [{ role: 'read', db: '<database>' }, " +
				"{ role: 'readAnyDatabase', db: 'admin' }])"}})
		return nil
	}
	run.Passed("mongo_change_stream", "Read change streams", checks.CatPermissions, "Can open change streams.", "")
	return nil
}

func containsStr(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// pyTypeName is type(v).__name__ of the value PyMongo would have decoded.
func pyTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "NoneType"
	case string:
		return "str"
	case int32, int:
		return "int"
	case int64:
		return "int" // PyMongo decodes int64 to int
	case float64:
		return "float"
	case bool:
		return "bool"
	case bson.D, bson.M:
		return "dict"
	case bson.A, []any:
		return "list"
	case bson.ObjectID:
		return "ObjectId"
	case bson.DateTime:
		return "datetime"
	case bson.Decimal128:
		return "Decimal128"
	case bson.Binary:
		return "bytes"
	case bson.Regex:
		return "Regex"
	case bson.Timestamp:
		return "Timestamp"
	case bson.JavaScript, bson.CodeWithScope:
		return "Code"
	case bson.MinKey:
		return "MinKey"
	case bson.MaxKey:
		return "MaxKey"
	case bson.Undefined:
		return "NoneType"
	}
	return fmt.Sprintf("%T", v)
}

func (s *mongoSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	client, done, err := mongoConnect(ctx, conn, 10*time.Second)
	if err != nil {
		return Discovery{}, err
	}
	defer done()
	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return Discovery{}, err
	}
	sort.Strings(dbs)
	var tables []TableInfo
	for _, dbName := range dbs {
		if mongoSystemDBs[dbName] {
			continue
		}
		db := client.Database(dbName)
		colls, err := listCollections(ctx, db)
		if err != nil {
			return Discovery{}, err
		}
		sort.Strings(colls)
		for _, coll := range colls {
			if strings.HasPrefix(coll, "system.") || len(tables) > mongoMaxCollections {
				continue
			}
			var sample bson.D
			err := db.Collection(coll).FindOne(ctx, bson.D{}).Decode(&sample)
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				return Discovery{}, err
			}
			columns := []Column{}
			for _, e := range sample {
				columns = append(columns, Column{Name: e.Key, Type: pyTypeName(e.Value), Nullable: true, PII: LooksLikePII(e.Key)})
			}
			var count *int64
			if n, err := db.Collection(coll).EstimatedDocumentCount(ctx); err == nil {
				count = &n
			}
			tables = append(tables, TableInfo{Schema: dbName, Name: coll, RowsEstimate: count, PrimaryKey: []string{"_id"}, Columns: columns, Selectable: true})
		}
	}
	truncated := len(tables) > mongoMaxCollections
	return NewDiscovery(first(tables, mongoMaxCollections), truncated), nil
}

func (s *mongoSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, sec, err := mongoCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	spec := c.Spec
	excludes, masks := ColumnRules(spec)
	if len(masks) > 0 {
		return nil, nil, false, problem.New(422, "masking_unsupported", "MongoDB fields cannot be masked; remove them instead.").
			WithErrors([]map[string]any{{"loc": []string{"spec", "source", "columns"}, "message": strings.Join(masks, ", ")}})
	}
	collections := append(append([]string{}, spec.Source.Tables.Include...), mongoSignalCollection(c))
	dbSet := map[string]bool{}
	for _, coll := range collections {
		db, _, _ := strings.Cut(coll, ".")
		dbSet[JavaRegexLiteral(db)] = true
	}
	dbs := make([]string, 0, len(dbSet))
	for d := range dbSet {
		dbs = append(dbs, d)
	}
	sort.Strings(dbs)
	props := map[string]string{
		"debezium.source.connector.class":           "io.debezium.connector.mongodb.MongoDbConnector",
		"debezium.source.mongodb.connection.string": "${BZ_MONGO_URI}",
		"debezium.source.collection.include.list":   joinRegex(collections),
		"debezium.source.database.include.list":     strings.Join(dbs, ","),
		"debezium.source.capture.mode":              cfg.CaptureMode,
		"debezium.source.signal.enabled.channels":   "source",
		"debezium.source.signal.data.collection":    mongoSignalCollection(c),
		"debezium.source.heartbeat.interval.ms":     strconv.Itoa(spec.Policies.HeartbeatSeconds * 1000),
	}
	if len(excludes) > 0 {
		props["debezium.source.field.exclude.list"] = strings.Join(excludes, ",")
	}
	return props, map[string]string{"BZ_MONGO_URI": MongoConnectionString(cfg, PyStr(sec["password"]))}, false, nil
}

func (s *mongoSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	dbName, coll, _ := strings.Cut(mongoSignalCollection(c), ".")
	client, done, err := mongoConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	names, err := listCollections(ctx, client.Database(dbName))
	if err != nil {
		return nil, err
	}
	if !containsStr(names, coll) {
		if err := client.Database(dbName).CreateCollection(ctx, coll); err != nil {
			return nil, err
		}
	}
	return []ArtifactResult{{Kind: "collection", Name: mongoSignalCollection(c), Outcome: "exists"}}, nil
}

func (s *mongoSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	dbName, coll, _ := strings.Cut(mongoSignalCollection(c), ".")
	client, done, err := mongoConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	names, err := listCollections(ctx, client.Database(dbName))
	if err != nil {
		return nil, err
	}
	existed := containsStr(names, coll)
	if err := client.Database(dbName).Collection(coll).Drop(ctx); err != nil {
		return nil, err
	}
	return []ArtifactResult{{Kind: "collection", Name: mongoSignalCollection(c), Outcome: pick(existed, "removed", "not_found"),
		Detail: "Change streams keep no server-side state; nothing else to release."}}, nil
}

func (s *mongoSource) Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error) {
	dbName, coll, _ := strings.Cut(mongoSignalCollection(c), ".")
	signalID := hex.EncodeToString(uuidBytes())
	client, done, err := mongoConnect(ctx, conn, 6*time.Second)
	if err != nil {
		return "", err
	}
	defer done()
	payload, err := PyDumps(data)
	if err != nil {
		return "", err
	}
	_, err = client.Database(dbName).Collection(coll).InsertOne(ctx, bson.D{{Key: "_id", Value: signalID}, {Key: "type", Value: signalType}, {Key: "data", Value: payload}})
	return signalID, err
}

func (s *mongoSource) Probe(ctx context.Context, conn Resolved, c CompileContext, _ int64) (*SourceImpact, error) {
	client, done, err := mongoConnect(ctx, conn, 5*time.Second)
	if err != nil {
		return &SourceImpact{Summary: fmt.Sprintf("Oplog not readable (%s).", firstRunes(err.Error(), 80))}, nil
	}
	defer done()
	oplog := client.Database("local").Collection("oplog.rs")
	var firstDoc, lastDoc struct {
		TS bson.Timestamp `bson:"ts"`
	}
	proj := options.FindOne().SetProjection(bson.D{{Key: "ts", Value: 1}})
	errFirst := oplog.FindOne(ctx, bson.D{}, proj.SetSort(bson.D{{Key: "$natural", Value: 1}})).Decode(&firstDoc)
	errLast := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetProjection(bson.D{{Key: "ts", Value: 1}}).SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&lastDoc)
	var stats bson.M
	errStats := client.Database("local").RunCommand(ctx, bson.D{{Key: "collStats", Value: "oplog.rs"}}).Decode(&stats)
	for _, e := range []error{errFirst, errLast, errStats} {
		if e != nil && !errors.Is(e, mongo.ErrNoDocuments) {
			return &SourceImpact{Summary: fmt.Sprintf("Oplog not readable (%s).", firstRunes(e.Error(), 80))}, nil
		}
	}
	if errors.Is(errFirst, mongo.ErrNoDocuments) || errors.Is(errLast, mongo.ErrNoDocuments) {
		return &SourceImpact{Summary: "Oplog not available (sharded cluster or missing permission)."}, nil
	}
	return mongoImpact(int64(firstDoc.TS.T), int64(lastDoc.TS.T), toInt64(stats["size"]), toInt64(stats["maxSize"])), nil
}

// mongoImpact reports the oplog: first and last entry times (Unix seconds), its size and its
// capacity (0 when the server does not say). The oplog is shared, so the size is the whole
// oplog's, not this pipeline's.
func mongoImpact(first, last, size, capacity int64) *SourceImpact {
	window := max(last-first, 0)
	oldest := time.Unix(first, 0).UTC()
	full := capacity != 0 && float64(size) >= float64(capacity)*0.9
	impact := &SourceImpact{RetainedBytes: &size}
	if capacity != 0 {
		h := max(capacity-size, 0)
		impact.HeadroomBytes = &h
	}
	if full {
		st := fmt.Sprintf("window %d h", window/3600)
		impact.LogStatus = &st
		impact.WindowSeconds = &window
		impact.Summary = fmt.Sprintf("Oplog keeps %d h %d min of changes (oldest %s UTC)", window/3600, window%3600/60, oldest.Format("2006-01-02 15:04"))
	} else {
		st := "filling"
		impact.LogStatus = &st
		impact.Summary = "The oplog is not full yet, so it still holds every change since it was created."
	}
	if full && window < 24*3600 {
		r := fmt.Sprintf("The oplog only covers %d h; a longer pause would force a re-snapshot.", window/3600)
		impact.Risk = &r
		impact.RiskFix = "Increase the oplog size (replSetResizeOplog) or minimum retention (oplogMinRetentionHours)."
	}
	return impact
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	case int:
		return int64(x)
	}
	return 0
}

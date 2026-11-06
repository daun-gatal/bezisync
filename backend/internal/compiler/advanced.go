package compiler

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Advanced properties are raw Debezium Server keys an Expert passes through. BeziSync owns
// identity, state, credentials, routing and every choice the wizard makes, so a raw key may only
// add what BeziSync doesn't set. ClassifyAdvanced is the one place that decides; saving, preflight
// and import all ask it, so they never disagree.

// AdvancedClass is what a raw key is to BeziSync.
type AdvancedClass string

const (
	// AdvancedAllowed is passed through as typed; BeziSync can't verify it.
	AdvancedAllowed AdvancedClass = "allowed"
	// AdvancedInvalid is not a key or value BeziSync can pass on.
	AdvancedInvalid AdvancedClass = "invalid"
	// AdvancedCredential looks like a credential: those live on connections, encrypted.
	AdvancedCredential AdvancedClass = "credential"
	// AdvancedForbidden is unsafe: it runs code or SQL, drops or skips data, or weakens TLS.
	AdvancedForbidden AdvancedClass = "forbidden"
	// AdvancedModelled is set by a choice in the wizard (the Fix step).
	AdvancedModelled AdvancedClass = "modelled"
	// AdvancedOwned is a key BeziSync sets for this pipeline.
	AdvancedOwned AdvancedClass = "owned"
)

// AdvancedVerdict is ClassifyAdvanced's answer for one key.
type AdvancedVerdict struct {
	Key   string
	Class AdvancedClass
	// Message says why a key is refused and what to do instead ("" when allowed).
	Message string
	// Fix is where the value is set instead: "tables", "sink", "transform", "review", "connection".
	Fix string
}

// Refused reports whether the key may not be set.
func (v AdvancedVerdict) Refused() bool { return v.Class != AdvancedAllowed }

const (
	maxAdvancedKey   = 255
	maxAdvancedValue = 4096
)

var (
	advancedKeyRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9_-]*(\.[a-zA-Z0-9_-]+)+$`)
	// Names that hold credentials, in any case and spelling.
	credentialRe = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[._-]?key|access[._-]?key|private[._-]?key|credential|jaas|keystore|truststore|sasl|bearer|oauth)`)
)

// forbiddenSource are debezium.source.* options that are never safe to pass, with why.
var forbiddenSource = map[string]string{
	"slot.drop.on.stop":                      "Dropping the replication slot lets the source discard WAL this pipeline hasn't read yet.",
	"slot.failover":                          "Failover slots are a source-side setting BeziSync manages with the slot.",
	"offset.mismatch.strategy":               "It can move the stored offset forward and skip changes.",
	"lsn.flush.mode":                         "It changes who confirms WAL to the slot and can make the source keep or discard WAL unexpectedly.",
	"flush.lsn.source":                       "It changes who confirms WAL to the slot and can make the source keep or discard WAL unexpectedly.",
	"lsn.flush.timeout.action":               "It changes what happens when WAL can't be confirmed, which can skip changes.",
	"lsn.flush.timeout.ms":                   "It changes what happens when WAL can't be confirmed, which can skip changes.",
	"database.initial.statements":            "It runs SQL on every new connection to the source.",
	"heartbeat.action.query":                 "It runs SQL on the source on every heartbeat. BeziSync runs its own heartbeat.",
	"replica.identity.autoset.values":        "It changes tables on the source (ALTER TABLE … REPLICA IDENTITY).",
	"database.sslfactory":                    "It can turn off certificate checks. TLS is set on the connection.",
	"database.connection.factory.class":      "It loads a class into the connector. Custom code comes only from Admin-registered images.",
	"include.unknown.datatypes":              "It sends opaque bytes whose layout changes between Debezium versions.",
	"converters":                             "It loads converter classes into the connector. Custom code comes only from Admin-registered images.",
	"post.processors":                        "It loads post-processor classes into the connector. Custom code comes only from Admin-registered images.",
	"topic.naming.strategy":                  "It loads a naming class and changes where events go. Destinations follow the pipeline's naming.",
	"custom.sanitize.pattern":                "It changes which values Debezium hides in its logs, so secrets could be logged.",
	"snapshot.select.statement.overrides":    "It replaces the queries of the first copy, which could read columns you excluded.",
	"event.processing.failure.handling.mode": "It decides whether failing events are dropped. The pipeline's error policy sets it (Review → Errors).",
}

// forbiddenSourceRe are option families that are never safe to pass.
var forbiddenSourceRe = []struct {
	re  *regexp.Regexp
	why string
}{
	{regexp.MustCompile(`\.custom\.name$`), "It loads a custom implementation into the connector. Custom code comes only from Admin-registered images."},
	{regexp.MustCompile(`^internal\.`), "Internal options are not part of Debezium's supported configuration."},
	{regexp.MustCompile(`^snapshot\.select\.statement\.overrides\.`), "It replaces the queries of the first copy, which could read columns you excluded."},
}

// modelledSource are debezium.source.* option families the wizard sets, and where.
var modelledSource = []struct {
	re       *regexp.Regexp
	fix, why string
}{
	{regexp.MustCompile(`^(table|schema|database|collection)\.(include|exclude)\.list$`), "tables", "Tables are chosen on the Tables step."},
	{regexp.MustCompile(`^column\.|^field\.(include|exclude)\.list$|^message\.key\.columns$`), "tables", "Columns, personal data and keys are chosen on the Tables and Transform steps, so personal-data choices can't be bypassed."},
	{regexp.MustCompile(`^skipped\.operations$`), "transform", "Which changes to keep is a filter on the Transform step."},
	{regexp.MustCompile(`^snapshot\.mode`), "review", "How the first copy runs is chosen on Review."},
	{regexp.MustCompile(`^(heartbeat\.|signal\.|notification\.)`), "review", "Heartbeats, signals and notifications are part of source protection, which BeziSync runs."},
	{regexp.MustCompile(`^(errors\.|retriable\.restart)`), "review", "Retries follow the pipeline's error policy (Review → Errors)."},
	{regexp.MustCompile(`^database\.(hostname|port|user|dbname|names|server\.id|server\.name)$|^database\.ssl`), "connection", "Connection details are set on the source connection."},
	{regexp.MustCompile(`^(offset\.storage|schema\.history|topic\.|slot\.|publication\.|plugin\.name$|connector\.class$)`), "review", "BeziSync sets it: it owns state, identity and routing."},
}

// Options Debezium hands to the source's JDBC driver (driver.*, and database.* for the JDBC
// sources) that read local files, load classes, call out, log to files or weaken TLS. TLS and
// authentication are set on the connection.
var (
	jdbcPassthroughRe = regexp.MustCompile(`^(driver|database)\.(.+)$`)
	jdbcUnsafeRe      = regexp.MustCompile(`(?i)(localinfile|deserializ|interceptor|factory|class|plugin|callback|provider|transform|logger|log(file|path|level|writer)|file|path|dir|url|wallet|ssl|tls|trust|certificate|cert|keystore|cipher|kerberos|gss|krb|jaas|sspi|authentication|publickey|rsa|ha\.|loadbalance|xml|tns|options$|initsql|init_sql|sessionvariables|propertiestransform|profiler|connectionattributes)`)
)

// destinationRe are last parts of debezium.sink.* keys that decide where data goes.
var destinationRe = regexp.MustCompile(`(?i)(url|uri|endpoints?|address(es)?|hosts?|hostname|servers?|brokers?|region|project|account|tenant|namespace|bucket|warehouse|catalog|stream|topic|queue|hub|channel|subject|database|table|keyspace)$`)

// ClassifyAdvanced says what a raw Debezium Server key is. managed holds the keys BeziSync
// compiled for this pipeline (nil when unknown, as when saving): those are always refused.
func ClassifyAdvanced(key, value string, managed map[string]bool) AdvancedVerdict {
	v := AdvancedVerdict{Key: key, Class: AdvancedAllowed}
	refuse := func(c AdvancedClass, fix, msg string) AdvancedVerdict {
		v.Class, v.Fix, v.Message = c, fix, msg
		return v
	}
	if len(key) > maxAdvancedKey {
		return refuse(AdvancedInvalid, "", fmt.Sprintf("Keys are at most %d characters.", maxAdvancedKey))
	}
	if !advancedKeyRe.MatchString(key) {
		return refuse(AdvancedInvalid, "", "Use a property name of lower-case ASCII parts separated by dots, like debezium.source.poll.interval.ms.")
	}
	if len(value) > maxAdvancedValue {
		return refuse(AdvancedInvalid, "", fmt.Sprintf("Values are at most %d characters.", maxAdvancedValue))
	}
	if strings.Contains(value, "${") {
		return refuse(AdvancedInvalid, "", "Values can't contain ${…}: Debezium Server would fill it in from the pod's environment, which holds credentials.")
	}
	if strings.IndexFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return refuse(AdvancedInvalid, "", "Values can't contain line breaks, tabs or other control characters.")
	}
	if credentialRe.MatchString(key) {
		return refuse(AdvancedCredential, "connection", "Credentials belong on the connection, where they are encrypted and write-only.")
	}
	switch {
	case strings.HasPrefix(key, "debezium.source."):
		opt := strings.TrimPrefix(key, "debezium.source.")
		if why, ok := forbiddenSource[opt]; ok {
			return refuse(AdvancedForbidden, "", why)
		}
		for _, f := range forbiddenSourceRe {
			if f.re.MatchString(opt) {
				return refuse(AdvancedForbidden, "", f.why)
			}
		}
		for _, m := range modelledSource {
			if m.re.MatchString(opt) {
				return refuse(AdvancedModelled, m.fix, m.why)
			}
		}
		if m := jdbcPassthroughRe.FindStringSubmatch(opt); m != nil && jdbcUnsafeRe.MatchString(m[2]) {
			return refuse(AdvancedForbidden, "connection", "This JDBC driver option can read local files, load classes, log to files or change TLS, so it isn't passed through. TLS and sign-in are set on the connection.")
		}
	case strings.HasPrefix(key, "debezium.sink."):
		last := key[strings.LastIndex(key, ".")+1:]
		if destinationRe.MatchString(last) {
			return refuse(AdvancedModelled, "sink", "Where events go comes from the destination connection, which an Admin approved.")
		}
		if strings.Contains(key, ".ssl.") || strings.Contains(key, ".security.") || strings.HasSuffix(key, ".security.protocol") {
			return refuse(AdvancedModelled, "connection", "TLS and security settings are set on the destination connection.")
		}
	case strings.HasPrefix(key, "debezium.format."):
		return refuse(AdvancedModelled, "sink", "The event format is chosen on the Destination step.")
	case strings.HasPrefix(key, "debezium.transforms") || strings.HasPrefix(key, "debezium.predicates"):
		return refuse(AdvancedModelled, "transform", "Transforms are steps on the Transform step, where they are previewed and policy-checked.")
	case strings.HasPrefix(key, "quarkus."):
		return refuse(AdvancedForbidden, "", "Quarkus settings are not passed through: BeziSync sets the server's HTTP, logging and health.")
	default:
		return refuse(AdvancedInvalid, "", "Raw properties start with debezium.source. or debezium.sink.")
	}
	for _, p := range ManagedPatterns {
		if p.MatchString(key) {
			return refuse(AdvancedOwned, "", "BeziSync sets this for every pipeline and it can't be overridden.")
		}
	}
	if managed[key] {
		return refuse(AdvancedOwned, "", "BeziSync sets this for this pipeline from its other settings, so it can't be overridden here.")
	}
	return v
}

// AdvancedRefusals classifies every key of advanced (sorted), keeping only the refused ones.
func AdvancedRefusals(advanced map[string]string, managed map[string]bool) []AdvancedVerdict {
	keys := make([]string, 0, len(advanced))
	for k := range advanced {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var out []AdvancedVerdict
	for _, k := range keys {
		if v := ClassifyAdvanced(k, advanced[k], managed); v.Refused() {
			out = append(out, v)
		}
	}
	return out
}

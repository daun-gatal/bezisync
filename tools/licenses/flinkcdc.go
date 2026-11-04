package licenses

// FlinkCDCImage is the reference Dockerfile of the optional Flink CDC engine image (Admins build
// it; BeziSync does not publish it), relative to the repository root.
const FlinkCDCImage = "deploy/images/flink-cdc/Dockerfile"

// Artifact is a third-party file the Flink CDC image downloads.
type Artifact struct {
	File string // the file name in the download URL
	SPDX string // its licence, reviewed by a maintainer
}

// FlinkCDCArtifacts are every file the reference image downloads by default, each reviewed. The
// image copies each JAR's own LICENSE and NOTICE files under /licenses. A download not listed here
// fails TestFlinkCDCImageArtifactsReviewed. MySQL Connector/J (GPL-2.0 with the Universal FOSS
// Exception) is added only when the Admin building the image asks for it (MYSQL_DRIVER).
var FlinkCDCArtifacts = []Artifact{
	{"flink-cdc-3.7.0-2.2-bin.tar.gz", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-mysql-3.7.0-2.2.jar", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-postgres-3.7.0-2.2.jar", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-sqlserver-3.7.0-2.2.jar", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-iceberg-3.7.0-2.2.jar", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-starrocks-3.7.0-2.2.jar", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-doris-3.7.0-2.2.jar", "Apache-2.0"},
	{"flink-cdc-pipeline-connector-fluss-3.7.0-2.2.jar", "Apache-2.0"},
	{"hadoop-client-api-3.3.6.jar", "Apache-2.0"},
	{"hadoop-client-runtime-3.3.6.jar", "Apache-2.0"},
	{"commons-logging-1.3.5.jar", "Apache-2.0"},
	{"flink-shaded-guava-31.1-jre-17.0.jar", "Apache-2.0"},
}

// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generator

// microgenConfig represents a single microgen target.
type microgenConfig struct {
	// inputDirectoryPath is the path to the input (.proto, etc) files, relative
	// to googleapisDir.
	inputDirectoryPath string

	// importPath is the path that this library should be imported as.
	importPath string

	// pkg is the name that should be used in the package declaration.
	pkg string

	// gRPCServiceConfigPath is the path to the grpc service config for this
	// target, relative to googleapisDir.
	gRPCServiceConfigPath string

	// apiServiceConfigPath is the path to the gapic service config for this
	// target, relative to googleapisDir.
	apiServiceConfigPath string

	// releaseLevel is the release level of this target. Values incl ga,
	// beta, alpha.
	releaseLevel string

	// stopGeneration is used to stop generating a given client. This might be
	// useful if a client needs to be deprecated, but retained in the repo
	// metadata.
	stopGeneration bool
}

var microgenGapicConfigs = []*microgenConfig{
	{
		inputDirectoryPath:    "google/cloud/texttospeech/v1",
		pkg:                   "texttospeech",
		importPath:            "cloud.google.com/go/texttospeech/apiv1",
		gRPCServiceConfigPath: "google/cloud/texttospeech/v1/texttospeech_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/texttospeech/v1/texttospeech_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/asset/v1",
		pkg:                   "asset",
		importPath:            "cloud.google.com/go/asset/apiv1",
		gRPCServiceConfigPath: "google/cloud/asset/v1/cloudasset_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/asset/v1/cloudasset_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/billing/v1",
		pkg:                   "billing",
		importPath:            "cloud.google.com/go/billing/apiv1",
		gRPCServiceConfigPath: "google/cloud/billing/v1/cloud_billing_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/billing/v1/cloudbilling.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/language/v1",
		pkg:                   "language",
		importPath:            "cloud.google.com/go/language/apiv1",
		gRPCServiceConfigPath: "google/cloud/language/v1/language_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/language/language_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/language/v1beta2",
		pkg:                   "language",
		importPath:            "cloud.google.com/go/language/apiv1beta2",
		gRPCServiceConfigPath: "google/cloud/language/v1beta2/language_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/language/v1beta2/language_v1beta2.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/memcache/v1beta2",
		pkg:                   "memcache",
		importPath:            "cloud.google.com/go/memcache/apiv1beta2",
		gRPCServiceConfigPath: "google/cloud/memcache/v1beta2/memcache_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/memcache/v1beta2/memcache_v1beta2.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/phishingprotection/v1beta1",
		pkg:                   "phishingprotection",
		importPath:            "cloud.google.com/go/phishingprotection/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/phishingprotection/v1beta1/phishingprotection_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/phishingprotection/v1beta1/phishingprotection_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/translate/v3",
		pkg:                   "translate",
		importPath:            "cloud.google.com/go/translate/apiv3",
		gRPCServiceConfigPath: "google/cloud/translate/v3/translate_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/translate/v3/translate_v3.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/scheduler/v1",
		pkg:                   "scheduler",
		importPath:            "cloud.google.com/go/scheduler/apiv1",
		gRPCServiceConfigPath: "google/cloud/scheduler/v1/cloudscheduler_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/scheduler/v1/cloudscheduler_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/scheduler/v1beta1",
		pkg:                   "scheduler",
		importPath:            "cloud.google.com/go/scheduler/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/scheduler/v1beta1/cloudscheduler_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/scheduler/v1beta1/cloudscheduler_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/speech/v1",
		pkg:                   "speech",
		importPath:            "cloud.google.com/go/speech/apiv1",
		gRPCServiceConfigPath: "google/cloud/speech/v1/speech_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/speech/v1/speech_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/speech/v1p1beta1",
		pkg:                   "speech",
		importPath:            "cloud.google.com/go/speech/apiv1p1beta1",
		gRPCServiceConfigPath: "google/cloud/speech/v1p1beta1/speech_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/speech/v1p1beta1/speech_v1p1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/connection/v1beta1",
		pkg:                   "connection",
		importPath:            "cloud.google.com/go/bigquery/connection/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/bigquery/connection/v1beta1/bigqueryconnection_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/connection/v1beta1/bigqueryconnection_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/connection/v1",
		pkg:                   "connection",
		importPath:            "cloud.google.com/go/bigquery/connection/apiv1",
		gRPCServiceConfigPath: "google/cloud/bigquery/connection/v1/bigqueryconnection_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/connection/v1/bigqueryconnection_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/datatransfer/v1",
		pkg:                   "datatransfer",
		importPath:            "cloud.google.com/go/bigquery/datatransfer/apiv1",
		gRPCServiceConfigPath: "google/cloud/bigquery/datatransfer/v1/bigquerydatatransfer_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/datatransfer/v1/bigquerydatatransfer_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/reservation/v1beta1",
		pkg:                   "reservation",
		importPath:            "cloud.google.com/go/bigquery/reservation/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/bigquery/reservation/v1beta1/bigqueryreservation_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/reservation/v1beta1/bigqueryreservation_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/reservation/v1",
		pkg:                   "reservation",
		importPath:            "cloud.google.com/go/bigquery/reservation/apiv1",
		gRPCServiceConfigPath: "google/cloud/bigquery/reservation/v1/bigqueryreservation_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/reservation/v1/bigqueryreservation_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/storage/v1alpha2",
		pkg:                   "storage",
		importPath:            "cloud.google.com/go/bigquery/storage/apiv1alpha2",
		gRPCServiceConfigPath: "google/cloud/bigquery/storage/v1alpha2/bigquerystorage_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/storage/v1alpha2/bigquerystorage_v1alpha2.yaml",
		releaseLevel:          "alpha",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/storage/v1beta1",
		pkg:                   "storage",
		importPath:            "cloud.google.com/go/bigquery/storage/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/bigquery/storage/v1beta1/bigquerystorage_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/storage/v1beta1/bigquerystorage_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/storage/v1beta2",
		pkg:                   "storage",
		importPath:            "cloud.google.com/go/bigquery/storage/apiv1beta2",
		gRPCServiceConfigPath: "google/cloud/bigquery/storage/v1beta2/bigquerystorage_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/storage/v1beta2/bigquerystorage_v1beta2.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/bigquery/storage/v1",
		pkg:                   "storage",
		importPath:            "cloud.google.com/go/bigquery/storage/apiv1",
		gRPCServiceConfigPath: "google/cloud/bigquery/storage/v1/bigquerystorage_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/bigquery/storage/v1/bigquerystorage_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/iot/v1",
		pkg:                   "iot",
		importPath:            "cloud.google.com/go/iot/apiv1",
		gRPCServiceConfigPath: "google/cloud/iot/v1/cloudiot_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/iot/v1/cloudiot_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/recommender/v1beta1",
		pkg:                   "recommender",
		importPath:            "cloud.google.com/go/recommender/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/recommender/v1beta1/recommender_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/recommender/v1beta1/recommender_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/tasks/v2",
		pkg:                   "cloudtasks",
		importPath:            "cloud.google.com/go/cloudtasks/apiv2",
		gRPCServiceConfigPath: "google/cloud/tasks/v2/cloudtasks_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/tasks/v2/cloudtasks_v2.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/tasks/v2beta2",
		pkg:                   "cloudtasks",
		importPath:            "cloud.google.com/go/cloudtasks/apiv2beta2",
		gRPCServiceConfigPath: "google/cloud/tasks/v2beta2/cloudtasks_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/tasks/v2beta2/cloudtasks_v2beta2.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/tasks/v2beta3",
		pkg:                   "cloudtasks",
		importPath:            "cloud.google.com/go/cloudtasks/apiv2beta3",
		gRPCServiceConfigPath: "google/cloud/tasks/v2beta3/cloudtasks_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/tasks/v2beta3/cloudtasks_v2beta3.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/videointelligence/v1",
		pkg:                   "videointelligence",
		importPath:            "cloud.google.com/go/videointelligence/apiv1",
		gRPCServiceConfigPath: "google/cloud/videointelligence/v1/videointelligence_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/videointelligence/v1/videointelligence_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/vision/v1",
		pkg:                   "vision",
		importPath:            "cloud.google.com/go/vision/apiv1",
		gRPCServiceConfigPath: "google/cloud/vision/v1/vision_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/vision/v1/vision_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/webrisk/v1",
		pkg:                   "webrisk",
		importPath:            "cloud.google.com/go/webrisk/apiv1",
		gRPCServiceConfigPath: "google/cloud/webrisk/v1/webrisk_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/webrisk/v1/webrisk_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/webrisk/v1beta1",
		pkg:                   "webrisk",
		importPath:            "cloud.google.com/go/webrisk/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/webrisk/v1beta1/webrisk_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/webrisk/v1beta1/webrisk_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/secretmanager/v1",
		pkg:                   "secretmanager",
		importPath:            "cloud.google.com/go/secretmanager/apiv1",
		gRPCServiceConfigPath: "google/cloud/secretmanager/v1/secretmanager_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/secretmanager/v1/secretmanager_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/secrets/v1beta1",
		pkg:                   "secretmanager",
		importPath:            "cloud.google.com/go/secretmanager/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/secrets/v1beta1/secretmanager_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/secrets/v1beta1/secretmanager_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/osconfig/v1",
		pkg:                   "osconfig",
		importPath:            "cloud.google.com/go/osconfig/apiv1",
		gRPCServiceConfigPath: "google/cloud/osconfig/v1/osconfig_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/osconfig/v1/osconfig_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/osconfig/v1beta",
		pkg:                   "osconfig",
		importPath:            "cloud.google.com/go/osconfig/apiv1beta",
		gRPCServiceConfigPath: "google/cloud/osconfig/v1beta/osconfig_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/osconfig/v1beta/osconfig_v1beta.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/osconfig/agentendpoint/v1",
		pkg:                   "agentendpoint",
		importPath:            "cloud.google.com/go/osconfig/agentendpoint/apiv1",
		gRPCServiceConfigPath: "google/cloud/osconfig/agentendpoint/v1/agentendpoint_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/osconfig/agentendpoint/v1/osconfig_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/osconfig/agentendpoint/v1beta",
		pkg:                   "agentendpoint",
		importPath:            "cloud.google.com/go/osconfig/agentendpoint/apiv1beta",
		gRPCServiceConfigPath: "google/cloud/osconfig/agentendpoint/v1beta/agentendpoint_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/osconfig/agentendpoint/v1beta/osconfig_v1beta.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/datacatalog/v1",
		pkg:                   "datacatalog",
		importPath:            "cloud.google.com/go/datacatalog/apiv1",
		gRPCServiceConfigPath: "google/cloud/datacatalog/v1/datacatalog_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/datacatalog/v1/datacatalog_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/datacatalog/v1beta1",
		pkg:                   "datacatalog",
		importPath:            "cloud.google.com/go/datacatalog/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/datacatalog/v1beta1/datacatalog_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/datacatalog/v1beta1/datacatalog_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/dataproc/v1",
		pkg:                   "dataproc",
		importPath:            "cloud.google.com/go/dataproc/apiv1",
		gRPCServiceConfigPath: "google/cloud/dataproc/v1/dataproc_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/dataproc/v1/dataproc_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/dataproc/v1beta2",
		pkg:                   "dataproc",
		importPath:            "cloud.google.com/go/dataproc/apiv1beta2",
		gRPCServiceConfigPath: "google/cloud/dataproc/v1beta2/dataproc_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/dataproc/v1beta2/dataproc_v1beta2.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/kms/v1",
		pkg:                   "kms",
		importPath:            "cloud.google.com/go/kms/apiv1",
		gRPCServiceConfigPath: "google/cloud/kms/v1/cloudkms_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/kms/v1/cloudkms_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/oslogin/v1",
		pkg:                   "oslogin",
		importPath:            "cloud.google.com/go/oslogin/apiv1",
		gRPCServiceConfigPath: "google/cloud/oslogin/v1/oslogin_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/oslogin/v1/oslogin_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/oslogin/v1beta",
		pkg:                   "oslogin",
		importPath:            "cloud.google.com/go/oslogin/apiv1beta",
		gRPCServiceConfigPath: "google/cloud/oslogin/v1beta/oslogin_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/oslogin/v1beta/oslogin_v1beta.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/recaptchaenterprise/v1",
		pkg:                   "recaptchaenterprise",
		importPath:            "cloud.google.com/go/recaptchaenterprise/apiv1",
		gRPCServiceConfigPath: "google/cloud/recaptchaenterprise/v1/recaptchaenterprise_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/recaptchaenterprise/v1/recaptchaenterprise_v1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/recaptchaenterprise/v1beta1",
		pkg:                   "recaptchaenterprise",
		importPath:            "cloud.google.com/go/recaptchaenterprise/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/recaptchaenterprise/v1beta1/recaptchaenterprise_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/recaptchaenterprise/v1beta1/recaptchaenterprise_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/redis/v1",
		pkg:                   "redis",
		importPath:            "cloud.google.com/go/redis/apiv1",
		gRPCServiceConfigPath: "google/cloud/redis/v1/redis_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/redis/v1/redis_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/redis/v1beta1",
		pkg:                   "redis",
		importPath:            "cloud.google.com/go/redis/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/redis/v1beta1/redis_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/redis/v1beta1/redis_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/devtools/clouddebugger/v2",
		pkg:                   "debugger",
		importPath:            "cloud.google.com/go/debugger/apiv2",
		gRPCServiceConfigPath: "google/devtools/clouddebugger/v2/clouddebugger_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/clouddebugger/v2/clouddebugger_v2.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/devtools/clouderrorreporting/v1beta1",
		pkg:                   "errorreporting",
		importPath:            "cloud.google.com/go/errorreporting/apiv1beta1",
		gRPCServiceConfigPath: "google/devtools/clouderrorreporting/v1beta1/errorreporting_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/clouderrorreporting/v1beta1/clouderrorreporting_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/devtools/cloudtrace/v1",
		pkg:                   "trace",
		importPath:            "cloud.google.com/go/trace/apiv1",
		gRPCServiceConfigPath: "google/devtools/cloudtrace/v1/cloudtrace_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/cloudtrace/v1/cloudtrace_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/devtools/cloudtrace/v2",
		pkg:                   "trace",
		importPath:            "cloud.google.com/go/trace/apiv2",
		gRPCServiceConfigPath: "google/devtools/cloudtrace/v2/cloudtrace_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/cloudtrace/v2/cloudtrace_v2.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/privacy/dlp/v2",
		pkg:                   "dlp",
		importPath:            "cloud.google.com/go/dlp/apiv2",
		gRPCServiceConfigPath: "google/privacy/dlp/v2/dlp_grpc_service_config.json",
		apiServiceConfigPath:  "google/privacy/dlp/v2/dlp_v2.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/datastore/admin/v1",
		pkg:                   "admin",
		importPath:            "cloud.google.com/go/datastore/admin/apiv1",
		gRPCServiceConfigPath: "google/datastore/admin/v1/datastore_admin_grpc_service_config.json",
		apiServiceConfigPath:  "google/datastore/admin/v1/datastore_admin_v1.yaml",
		releaseLevel:          "alpha",
	},
	{
		inputDirectoryPath:    "google/spanner/admin/database/v1",
		pkg:                   "database",
		importPath:            "cloud.google.com/go/spanner/admin/database/apiv1",
		gRPCServiceConfigPath: "google/spanner/admin/database/v1/spanner_admin_database_grpc_service_config.json",
		apiServiceConfigPath:  "google/spanner/admin/database/v1/spanner_admin_database.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/spanner/admin/instance/v1",
		pkg:                   "instance",
		importPath:            "cloud.google.com/go/spanner/admin/instance/apiv1",
		gRPCServiceConfigPath: "google/spanner/admin/instance/v1/spanner_admin_instance_grpc_service_config.json",
		apiServiceConfigPath:  "google/spanner/admin/instance/v1/spanner_admin_instance.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/spanner/v1",
		pkg:                   "spanner",
		importPath:            "cloud.google.com/go/spanner/apiv1",
		gRPCServiceConfigPath: "google/spanner/v1/spanner_grpc_service_config.json",
		apiServiceConfigPath:  "google/spanner/v1/spanner.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/securitycenter/settings/v1beta1",
		pkg:                   "settings",
		importPath:            "cloud.google.com/go/securitycenter/settings/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/securitycenter/settings/v1beta1/securitycenter_settings_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/securitycenter/settings/v1beta1/securitycenter_settings.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/securitycenter/v1",
		pkg:                   "securitycenter",
		importPath:            "cloud.google.com/go/securitycenter/apiv1",
		gRPCServiceConfigPath: "google/cloud/securitycenter/v1/securitycenter_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/securitycenter/v1/securitycenter_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/securitycenter/v1beta1",
		pkg:                   "securitycenter",
		importPath:            "cloud.google.com/go/securitycenter/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/securitycenter/v1beta1/securitycenter_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/securitycenter/v1beta1/securitycenter_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/securitycenter/v1p1beta1",
		pkg:                   "securitycenter",
		importPath:            "cloud.google.com/go/securitycenter/apiv1p1beta1",
		gRPCServiceConfigPath: "google/cloud/securitycenter/v1p1beta1/securitycenter_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/securitycenter/v1p1beta1/securitycenter_v1p1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/firestore/admin/v1",
		pkg:                   "apiv1",
		importPath:            "cloud.google.com/go/firestore/apiv1/admin",
		gRPCServiceConfigPath: "google/firestore/admin/v1/firestore_admin_grpc_service_config.json",
		apiServiceConfigPath:  "google/firestore/admin/firestore_admin_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/firestore/v1",
		pkg:                   "firestore",
		importPath:            "cloud.google.com/go/firestore/apiv1",
		gRPCServiceConfigPath: "google/firestore/v1/firestore_grpc_service_config.json",
		apiServiceConfigPath:  "google/firestore/v1/firestore_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/devtools/cloudbuild/v1",
		pkg:                   "cloudbuild",
		importPath:            "cloud.google.com/go/cloudbuild/apiv1/v2",
		gRPCServiceConfigPath: "google/devtools/cloudbuild/v1/cloudbuild_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/cloudbuild/v1/cloudbuild_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/dialogflow/v2",
		pkg:                   "dialogflow",
		importPath:            "cloud.google.com/go/dialogflow/apiv2",
		gRPCServiceConfigPath: "google/cloud/dialogflow/v2/dialogflow_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/dialogflow/v2/dialogflow_v2.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/iam/credentials/v1",
		pkg:                   "credentials",
		importPath:            "cloud.google.com/go/iam/credentials/apiv1",
		gRPCServiceConfigPath: "google/iam/credentials/v1/iamcredentials_grpc_service_config.json",
		apiServiceConfigPath:  "google/iam/credentials/v1/iamcredentials_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/longrunning",
		pkg:                   "longrunning",
		importPath:            "cloud.google.com/go/longrunning/autogen",
		gRPCServiceConfigPath: "google/longrunning/longrunning_grpc_service_config.json",
		apiServiceConfigPath:  "google/longrunning/longrunning.yaml",
		releaseLevel:          "alpha",
	},
	{
		inputDirectoryPath:    "google/devtools/containeranalysis/v1beta1",
		pkg:                   "containeranalysis",
		importPath:            "cloud.google.com/go/containeranalysis/apiv1beta1",
		gRPCServiceConfigPath: "google/devtools/containeranalysis/v1beta1/containeranalysis_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/containeranalysis/v1beta1/containeranalysis_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		// The grafeas v1beta1 client must be generated in the same package as containeranalysis v1beta1,
		// but the proto is in a sub-directory of the containeranalysis v1beta1 protos.
		inputDirectoryPath:    "google/devtools/containeranalysis/v1beta1/grafeas",
		pkg:                   "containeranalysis",
		importPath:            "cloud.google.com/go/containeranalysis/apiv1beta1",
		gRPCServiceConfigPath: "google/devtools/containeranalysis/v1beta1/containeranalysis_grpc_service_config.json",
		apiServiceConfigPath:  "google/devtools/containeranalysis/v1beta1/containeranalysis_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/recommender/v1",
		pkg:                   "recommender",
		importPath:            "cloud.google.com/go/recommender/apiv1",
		gRPCServiceConfigPath: "google/cloud/recommender/v1/recommender_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/recommender/v1/recommender_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/videointelligence/v1beta2",
		pkg:                   "videointelligence",
		importPath:            "cloud.google.com/go/videointelligence/apiv1beta2",
		gRPCServiceConfigPath: "google/cloud/videointelligence/v1beta2/videointelligence_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/videointelligence/videointelligence_v1beta2.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/asset/v1beta1",
		pkg:                   "asset",
		importPath:            "cloud.google.com/go/asset/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/asset/v1beta1/cloudasset_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/asset/v1beta1/cloudasset_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/asset/v1p2beta1",
		pkg:                   "asset",
		importPath:            "cloud.google.com/go/asset/apiv1p2beta1",
		gRPCServiceConfigPath: "google/cloud/asset/v1p2beta1/cloudasset_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/asset/v1p2beta1/cloudasset_v1p2beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/asset/v1p5beta1",
		pkg:                   "asset",
		importPath:            "cloud.google.com/go/asset/apiv1p5beta1",
		gRPCServiceConfigPath: "google/cloud/asset/v1p5beta1/cloudasset_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/asset/v1p5beta1/cloudasset_v1p5beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/monitoring/v3",
		pkg:                   "monitoring",
		importPath:            "cloud.google.com/go/monitoring/apiv3/v2",
		gRPCServiceConfigPath: "google/monitoring/v3/monitoring_grpc_service_config.json",
		apiServiceConfigPath:  "google/monitoring/v3/monitoring.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/vision/v1p1beta1",
		pkg:                   "vision",
		importPath:            "cloud.google.com/go/vision/apiv1p1beta1",
		gRPCServiceConfigPath: "google/cloud/vision/v1p1beta1/vision_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/vision/v1p1beta1/vision_v1p1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/logging/v2",
		pkg:                   "logging",
		importPath:            "cloud.google.com/go/logging/apiv2",
		gRPCServiceConfigPath: "google/logging/v2/logging_grpc_service_config.json",
		apiServiceConfigPath:  "google/logging/v2/logging.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/talent/v4beta1",
		pkg:                   "talent",
		importPath:            "cloud.google.com/go/talent/apiv4beta1",
		gRPCServiceConfigPath: "google/cloud/talent/v4beta1/talent_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/talent/v4beta1/jobs_v4beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/pubsub/v1",
		pkg:                   "pubsub",
		importPath:            "cloud.google.com/go/pubsub/apiv1",
		gRPCServiceConfigPath: "google/pubsub/v1/pubsub_grpc_service_config.json",
		apiServiceConfigPath:  "google/pubsub/v1/pubsub_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/automl/v1",
		pkg:                   "automl",
		importPath:            "cloud.google.com/go/automl/apiv1",
		gRPCServiceConfigPath: "google/cloud/automl/v1/automl_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/automl/v1/automl_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/automl/v1beta1",
		pkg:                   "automl",
		importPath:            "cloud.google.com/go/automl/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/automl/v1beta1/automl_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/automl/v1beta1/automl_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/container/v1",
		pkg:                   "container",
		importPath:            "cloud.google.com/go/container/apiv1",
		gRPCServiceConfigPath: "google/container/v1/container_grpc_service_config.json",
		apiServiceConfigPath:  "google/container/v1/container_v1.yaml",
		releaseLevel:          "ga",
	},
	{
		inputDirectoryPath:    "google/cloud/servicedirectory/v1beta1",
		pkg:                   "servicedirectory",
		importPath:            "cloud.google.com/go/servicedirectory/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/servicedirectory/v1beta1/servicedirectory_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/servicedirectory/v1beta1/servicedirectory_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/gaming/v1beta",
		pkg:                   "gaming",
		importPath:            "cloud.google.com/go/gaming/apiv1beta",
		gRPCServiceConfigPath: "google/cloud/gaming/v1beta/gaming_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/gaming/v1beta/gaming_gapic.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/policytroubleshooter/v1",
		pkg:                   "policytroubleshooter",
		importPath:            "cloud.google.com/go/policytroubleshooter/apiv1",
		gRPCServiceConfigPath: "google/cloud/policytroubleshooter/v1/checker_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/policytroubleshooter/v1/policytroubleshooter_v1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/monitoring/dashboard/v1",
		pkg:                   "dashboard",
		importPath:            "cloud.google.com/go/monitoring/dashboard/apiv1",
		gRPCServiceConfigPath: "google/monitoring/dashboard/v1/dashboards_grpc_service_config.json",
		apiServiceConfigPath:  "google/monitoring/dashboard/v1/monitoring_gapic.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/functions/v1",
		pkg:                   "functions",
		importPath:            "cloud.google.com/go/functions/apiv1",
		gRPCServiceConfigPath: "google/cloud/functions/v1/functions_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/functions/v1/functions_gapic.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/notebooks/v1beta1",
		pkg:                   "notebooks",
		importPath:            "cloud.google.com/go/notebooks/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/notebooks/v1beta1/notebooks_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/notebooks/v1beta1/notebooks_v1beta1.yaml",
		releaseLevel:          "beta",
	},
	{
		inputDirectoryPath:    "google/cloud/billing/budgets/v1beta1",
		pkg:                   "budgets",
		importPath:            "cloud.google.com/go/billing/budgets/apiv1beta1",
		gRPCServiceConfigPath: "google/cloud/billing/budgets/v1beta1/billingbudgets_grpc_service_config.json",
		apiServiceConfigPath:  "google/cloud/billing/budgets/v1beta1/billingbudgets_gapic.yaml",
		releaseLevel:          "beta",
	},
}

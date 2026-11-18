{{/*
Expand the name of the chart.
*/}}
{{- define "pgmqtt.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pgmqtt.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "pgmqtt.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pgmqtt.labels" -}}
helm.sh/chart: {{ include "pgmqtt.chart" . }}
app.kubernetes.io/name: {{ include "pgmqtt.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "pgmqtt.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pgmqtt.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "pgmqtt.serviceHost" -}}
{{- if .Values.operator.serviceHost -}}
{{- .Values.operator.serviceHost -}}
{{- else -}}
{{- printf "%s.%s.svc.cluster.local" (include "pgmqtt.fullname" .) .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{/*
The binary serves /metrics and /healthz/* from one HTTP server, so the
server has to stay up for HTTP probes even when metrics.enabled=false.
Renders "true" or nothing.
*/}}
{{- define "pgmqtt.httpServerEnabled" -}}
{{- if or .Values.metrics.enabled (and .Values.probes.enabled .Values.probes.useHTTP) -}}
true
{{- end -}}
{{- end -}}

{{/*
Name of the cluster-scoped RBAC objects. fullname is only unique within a
namespace, so two releases with the same name in different namespaces
would otherwise claim the same ClusterRole. The colon cannot occur in a
namespace or in fullname, which keeps "a-b" in "c" apart from "a" in "b-c".
*/}}
{{- define "pgmqtt.clusterScopedName" -}}
{{- printf "%s:%s" (include "pgmqtt.fullname" .) .Release.Namespace -}}
{{- end -}}

{{/*
Lease name for the embedded operator's leader election, at most 63
characters. Two releases in one namespace must not share a Lease, or only
one of them ever reconciles. A release whose fullname is "pgmqtt" keeps
the name the binary used to hard-code, so upgrading it does not elect a
second leader while old and new Pods overlap.
*/}}
{{- define "pgmqtt.leaderElectionID" -}}
{{- printf "%s-operator" (regexReplaceAll "[^a-z0-9]+$" (include "pgmqtt.fullname" . | trunc 54) "") -}}
{{- end -}}

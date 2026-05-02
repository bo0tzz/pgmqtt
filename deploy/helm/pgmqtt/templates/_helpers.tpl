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
app.kubernetes.io/component: broker
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

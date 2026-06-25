{{/*
Expand the name of the chart.
*/}}
{{- define "omnikube.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "omnikube.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "omnikube.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "omnikube.labels" -}}
helm.sh/chart: {{ include "omnikube.chart" . }}
{{ include "omnikube.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "omnikube.selectorLabels" -}}
app.kubernetes.io/name: {{ include "omnikube.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "omnikube.server.labels" -}}
{{ include "omnikube.labels" . }}
app.kubernetes.io/component: server
app: omnikube-server
{{- end }}

{{- define "omnikube.server.selectorLabels" -}}
{{ include "omnikube.selectorLabels" . }}
app.kubernetes.io/component: server
app: omnikube-server
{{- end }}

{{- define "omnikube.aggregator.labels" -}}
{{ include "omnikube.labels" . }}
app.kubernetes.io/component: aggregator
app: omnikube-aggregator
{{- end }}

{{- define "omnikube.aggregator.selectorLabels" -}}
{{ include "omnikube.selectorLabels" . }}
app.kubernetes.io/component: aggregator
app: omnikube-aggregator
{{- end }}

{{- define "omnikube.agent.labels" -}}
{{ include "omnikube.labels" . }}
app.kubernetes.io/component: agent
app: omnikube-agent
{{- end }}

{{- define "omnikube.agent.selectorLabels" -}}
{{ include "omnikube.selectorLabels" . }}
app.kubernetes.io/component: agent
app: omnikube-agent
{{- end }}

{{- define "omnikube.server.image" -}}
{{- printf "%s:%s" .Values.server.image.repository .Values.server.image.tag }}
{{- end }}

{{- define "omnikube.aggregator.image" -}}
{{- printf "%s:%s" .Values.aggregator.image.repository .Values.aggregator.image.tag }}
{{- end }}

{{- define "omnikube.agent.image" -}}
{{- printf "%s:%s" .Values.agent.image.repository .Values.agent.image.tag }}
{{- end }}

{{- define "omnikube.mockHighCpu.image" -}}
{{- printf "%s:%s" .Values.mockHighCpu.image.repository .Values.mockHighCpu.image.tag }}
{{- end }}

{{- define "omnikube.stress.cpu.image" -}}
{{- printf "%s:%s" .Values.stress.cpuStressDaemonSet.image.repository .Values.stress.cpuStressDaemonSet.image.tag }}
{{- end }}

{{- define "omnikube.stress.loadGenerator.image" -}}
{{- printf "%s:%s" .Values.stress.loadGenerator.image.repository .Values.stress.loadGenerator.image.tag }}
{{- end }}

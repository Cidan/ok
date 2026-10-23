{{- define "ok.name" -}}{{ .Release.Name }}{{- end -}}

{{- define "ok.labels" -}}
app.kubernetes.io/name: ok
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ok.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}
{{- define "ok.runnerImage" -}}{{ .Values.runnerImage.repository }}:{{ .Values.runnerImage.tag | default .Chart.AppVersion }}{{- end -}}

{{/* The label each copy of the deployer carries, and learns its color from. */}}
{{- define "ok.colorLabel" -}}deploy.ok.{{ .Values.domain }}/color{{- end -}}

{{/* The server's address inside the cluster, and the deployer's. */}}
{{- define "ok.serverAddr" -}}{{ include "ok.name" . }}-server.{{ .Release.Namespace }}.svc:{{ .Values.service.port }}{{- end -}}
{{- define "ok.deployAddr" -}}{{ include "ok.name" . }}-deploy.{{ .Release.Namespace }}.svc:8472{{- end -}}
{{- define "ok.secretsAddr" -}}{{ include "ok.name" . }}-secrets.{{ .Release.Namespace }}.svc:8472{{- end -}}

{{/* A ServiceAccount's subject, as its tokens name it. */}}
{{- define "ok.subject" -}}system:serviceaccount:{{ .root.Release.Namespace }}:{{ include "ok.name" .root }}-{{ .account }}{{- end -}}

{{/* What every pod mounts: ok.yaml. */}}
{{- define "ok.configVolumes" -}}
- name: config
  configMap:
    name: {{ include "ok.name" . }}-config
{{- end -}}
{{- define "ok.configMounts" -}}
- name: config
  mountPath: /etc/ok/config
  readOnly: true
{{- end -}}

{{/* The database's URL, with its password: the server's and the deployer's, which keep tables.
     Never a worker's, which runs a change's code and keeps none. */}}
{{- define "ok.databaseVolumes" -}}
{{- if not .Values.database.spanner.instance -}}
- name: database
  secret:
    secretName: {{ .Values.database.urlSecret.name }}
    items: [{key: {{ .Values.database.urlSecret.key }}, path: url}]
{{- end -}}
{{- end -}}
{{- define "ok.databaseMounts" -}}
{{- if not .Values.database.spanner.instance -}}
- name: database
  mountPath: /etc/ok/secrets/database
  readOnly: true
{{- end -}}
{{- end -}}

{{/* A worker's or the deployer's resources: its own, and, for a virtual checkout, the FUSE device
     resource its node's device plugin offers (fuseResource), which lets it open /dev/fuse. */}}
{{- define "ok.resources" -}}
{{- $r := deepCopy (default dict .resources) -}}
{{- if and (eq .checkout "virtual") .fuseResource -}}
{{- $limits := deepCopy (default dict $r.limits) -}}
{{- $_ := set $limits .fuseResource 1 -}}
{{- $_ := set $r "limits" $limits -}}
{{- end -}}
{{- if and .microvm .microvm.enabled .microvm.kvmResource -}}
{{- $limits := deepCopy (default dict $r.limits) -}}
{{- $_ := set $limits .microvm.kvmResource 1 -}}
{{- $_ := set $r "limits" $limits -}}
{{- end -}}
{{- if $r }}{{ toYaml $r }}{{ end -}}
{{- end -}}

{{/* A role's projected ServiceAccount token, with which it certifies its key at identity. */}}
{{- define "ok.tokenVolume" -}}
- name: ok-token
  projected:
    sources:
      - serviceAccountToken: {audience: ok, expirationSeconds: 3600, path: token}
{{- end -}}

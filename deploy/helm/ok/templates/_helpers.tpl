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

{{/* The resource a device plugin offers a device as, for a worker or the deployer (dict "root" .
     "resource" its own setting "device" fuse|kvm): its own setting, else the chart's device
     plugin's; "" for none, where the chart binds the device in from the node instead. */}}
{{- define "ok.deviceResource" -}}
{{- if .resource -}}
{{- .resource -}}
{{- else if .root.Values.devicePlugin.enabled -}}
{{- printf "%s/%s" .root.Values.devicePlugin.domain .device -}}
{{- end -}}
{{- end -}}

{{/* A worker's or the deployer's resources (dict "root" . "v" its values): its own, and, for a
     virtual checkout, the FUSE device resource (ok.deviceResource) that lets it open /dev/fuse,
     and, for runs in microVMs, the KVM one. */}}
{{- define "ok.resources" -}}
{{- $v := .v -}}
{{- $r := deepCopy (default dict $v.resources) -}}
{{- $fuse := include "ok.deviceResource" (dict "root" .root "resource" $v.fuseResource "device" "fuse") -}}
{{- if and (eq $v.checkout "virtual") $fuse -}}
{{- $limits := deepCopy (default dict $r.limits) -}}
{{- $_ := set $limits $fuse 1 -}}
{{- $_ := set $r "limits" $limits -}}
{{- end -}}
{{- if and $v.microvm $v.microvm.enabled -}}
{{- $kvm := include "ok.deviceResource" (dict "root" .root "resource" $v.microvm.kvmResource "device" "kvm") -}}
{{- if $kvm -}}
{{- $limits := deepCopy (default dict $r.limits) -}}
{{- $_ := set $limits $kvm 1 -}}
{{- $_ := set $r "limits" $limits -}}
{{- end -}}
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

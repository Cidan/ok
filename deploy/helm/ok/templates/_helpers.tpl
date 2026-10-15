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

{{/* A ServiceAccount's subject, as its tokens name it. */}}
{{- define "ok.subject" -}}system:serviceaccount:{{ .root.Release.Namespace }}:{{ include "ok.name" .root }}-{{ .account }}{{- end -}}

{{/* What every pod mounts: ok.yaml, and the database's URL. */}}
{{- define "ok.configVolumes" -}}
- name: config
  configMap:
    name: {{ include "ok.name" . }}-config
- name: database
  secret:
    secretName: {{ .Values.database.urlSecret.name }}
    items: [{key: {{ .Values.database.urlSecret.key }}, path: url}]
{{- end -}}
{{- define "ok.configMounts" -}}
- name: config
  mountPath: /etc/ok/config
  readOnly: true
- name: database
  mountPath: /etc/ok/secrets/database
  readOnly: true
{{- end -}}

{{/* A role's projected ServiceAccount token, with which it certifies its key at identity. */}}
{{- define "ok.tokenVolume" -}}
- name: ok-token
  projected:
    sources:
      - serviceAccountToken: {audience: ok, expirationSeconds: 3600, path: token}
{{- end -}}

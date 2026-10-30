{{- define "agent.name" -}}{{ .Release.Name }}-agent{{- end -}}

{{- define "agent.labels" -}}
app.kubernetes.io/name: ok-agent
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "agent.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}

{{/* The resource migrations' FUSE device is offered as: fuseResource, else the chart's device
     plugin's; "" for none, where /dev/fuse is bound in from the node instead. */}}
{{- define "agent.fuseResource" -}}
{{- if .Values.migrations.fuseResource -}}
{{- .Values.migrations.fuseResource -}}
{{- else if .Values.devicePlugin.enabled -}}
{{- printf "%s/fuse" .Values.devicePlugin.domain -}}
{{- end -}}
{{- end -}}

{{/* The agent's resources: its own, and, running migrations, the FUSE device. */}}
{{- define "agent.resources" -}}
{{- $r := deepCopy (default dict .Values.resources) -}}
{{- $fuse := include "agent.fuseResource" . -}}
{{- if and .Values.migrations.enabled $fuse -}}
{{- $limits := deepCopy (default dict $r.limits) -}}
{{- $_ := set $limits $fuse 1 -}}
{{- $_ := set $r "limits" $limits -}}
{{- end -}}
{{- toYaml $r -}}
{{- end -}}

{{/* The agent's security context: its own, and, running migrations — which okfs presents, in a user
     namespace it mounts in — seccomp and AppArmor profiles that allow it, unless its own name some. */}}
{{- define "agent.securityContext" -}}
{{- $s := deepCopy (default dict .Values.securityContext) -}}
{{- if .Values.migrations.enabled -}}
{{- if not (hasKey $s "seccompProfile") }}{{ $_ := set $s "seccompProfile" (dict "type" "Unconfined") }}{{ end -}}
{{- if not (hasKey $s "appArmorProfile") }}{{ $_ := set $s "appArmorProfile" (dict "type" "Unconfined") }}{{ end -}}
{{- end -}}
{{- toYaml $s -}}
{{- end -}}

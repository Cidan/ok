package web

// Provider is an OpenID Connect provider's sign-in button.
type Provider struct {
	ID, Name string
}

// LoginView is the sign-in page.
type LoginView struct {
	Installation string
	Next         string // where to go after
	NextQuery    string // Next, query-escaped
	Error        string
	Providers    []Provider
	LDAP         bool
	LDAPName     string
	Passkeys     bool
}

// DeviceView asks for a device's code.
type DeviceView struct {
	Installation, Principal, Code, Error string
}

// DeviceConfirmView asks to approve a device.
type DeviceConfirmView struct {
	Installation, Principal, Code, Label, From, Asked string
}

// CredentialRow is one of a person's devices.
type CredentialRow struct {
	ID, Label, Detail string
	Revocable         bool
}

// PasskeyRow is one of a person's passkeys.
type PasskeyRow struct {
	ID, Label, Detail string
}

// AccountView is a person's account.
type AccountView struct {
	Installation, Principal, Email, Error string
	Credentials                           []CredentialRow
	Passkeys                              []PasskeyRow
	PasskeysOn                            bool
	Admin                                 bool
}

// WaitingRow is a person or group the sync could not take.
type WaitingRow struct {
	ExternalID, Reason, Placeholder string
	Group                           bool
}

// AdminView is the administrators' page.
type AdminView struct {
	Installation, Source, LastRun, SyncError, Error string
	Waiting                                         []WaitingRow
	Notes, Changes                                  []string
}

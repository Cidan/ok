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
	// Password: a hosted service's people sign in with their email and password; SignUp: they make
	// accounts.
	Password, SignUp bool
}

// SignUpView makes an account.
type SignUpView struct {
	Installation, Error, Email, Handle string
	Providers                          []Provider
	EmailOn                            bool
}

// ConfirmView confirms what an emailed link is for.
type ConfirmView struct {
	Installation, Title, Text, Action, Token, Button string
}

// ResetView asks for an account's email.
type ResetView struct {
	Installation, Error string
}

// NewPasswordView sets a password after a reset's link.
type NewPasswordView struct {
	Installation, Token, Error string
}

// HandleView chooses a provider's sign-up's handle.
type HandleView struct {
	Installation, Ceremony, Handle, Provider, Error string
}

// DeviceView asks for a device's code.
type DeviceView struct {
	Installation, Principal, Code, Error string
}

// ApproveView asks a person to approve what ok secrets asked, with their passkey.
type ApproveView struct {
	Installation, Principal, ID, Description, Asked, Expires string
	PasskeysOn                                               bool
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
	Tokens                                []TokenRow
	// NewToken is a token's secret just made, shown this once.
	NewToken string
	// PasswordOn: the account may have a password (a hosted service's), HasPassword whether it has
	// one; Links are the providers it may sign in with, linked or not.
	PasswordOn, HasPassword bool
	Links                   []LinkRow
	// Deletable: the person may delete their account here (a hosted service's own account).
	Deletable bool
}

// LinkRow is a provider an account may sign in with.
type LinkRow struct {
	Provider, Name string
	Linked         bool
}

// TokenRow is one of a person's personal tokens.
type TokenRow struct {
	ID, Label, Detail string
	Revocable         bool
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

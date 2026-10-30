package natsbus

// ForgetStreams is what a connection that came back does.
func ForgetStreams(b *Bus) { b.forgetStreams() }

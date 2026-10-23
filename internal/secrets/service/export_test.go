package service

import "time"

// SetTTLs sets how long the policy keeps main's head and a membership: tests'.
func (p *Policy) SetTTLs(head, member time.Duration) { p.headTTL, p.memberTTL = head, member }

// NextDue is nextDue.
var NextDue = nextDue

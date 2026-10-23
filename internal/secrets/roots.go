package secrets

import (
	"fmt"
	"regexp"
	"strings"
)

// An organization's own root (docs/secrets.md, "Keys"): a key in its own key management service,
// which ok's workload is granted to use — never a keyring, which is a file on the installation's
// machines.
var (
	gcpKey = regexp.MustCompile(`^projects/[a-z][-a-z0-9]{4,28}[a-z0-9]/locations/[a-z0-9-]{1,63}/keyRings/[A-Za-z0-9_-]{1,63}/cryptoKeys/[A-Za-z0-9_-]{1,63}$`)
	awsKey = regexp.MustCompile(`^arn:aws[a-z-]{0,20}:kms:[a-z0-9-]{1,32}:[0-9]{12}:key/[A-Za-z0-9-]{1,64}$`)
)

// ValidOrganizationRoot reports why id cannot be an organization's root, or nil: gcp-kms:<a
// crypto key's resource name>, or aws-kms:<a key's ARN>.
func ValidOrganizationRoot(id string) error {
	scheme, name, _ := strings.Cut(id, ":")
	switch scheme {
	case "gcp-kms":
		if gcpKey.MatchString(name) {
			return nil
		}
		return fmt.Errorf("%q is not a Cloud KMS crypto key: gcp-kms:projects/<project>/locations/<location>/keyRings/<ring>/cryptoKeys/<key>", id)
	case "aws-kms":
		if awsKey.MatchString(name) {
			return nil
		}
		return fmt.Errorf("%q is not an AWS KMS key: aws-kms:arn:aws:kms:<region>:<account>:key/<id>", id)
	case "keyring":
		return fmt.Errorf("%q is a keyring, a file on the installation's machines: an organization's root is a key in its own KMS", id)
	}
	return fmt.Errorf("%q is not a root: gcp-kms:<crypto key>, or aws-kms:<key ARN>", id)
}

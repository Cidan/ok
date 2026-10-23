package secrets

import "testing"

func TestValidOrganizationRoot(t *testing.T) {
	for _, ok := range []string{
		"gcp-kms:projects/acme-prod/locations/us/keyRings/ok/cryptoKeys/root",
		"aws-kms:arn:aws:kms:us-east-1:111111111111:key/00000000-0000-0000-0000-000000000001",
		"aws-kms:arn:aws-us-gov:kms:us-gov-west-1:222222222222:key/mrk-1234abcd",
	} {
		if err := ValidOrganizationRoot(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"", "keyring:/etc/ok/keyring", "gcp-kms:", "gcp-kms:projects/x/locations/us/keyRings/r/cryptoKeys/k",
		"gcp-kms:projects/acme-prod/locations/us/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1",
		"aws-kms:arn:aws:kms:us-east-1:1:key/x", "aws-kms:alias/root", "vault:secret/x",
		"gcp-kms:projects/acme-prod/locations/us/keyRings/r/cryptoKeys/k\n",
	} {
		if ValidOrganizationRoot(bad) == nil {
			t.Errorf("%q taken", bad)
		}
	}
}

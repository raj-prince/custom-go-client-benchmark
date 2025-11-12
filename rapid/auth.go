package rapid

import (
	"context"
	"fmt"
	"os"

	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Create token source from the JSON file at the supplide path.
func newTokenSourceFromPath(
	ctx context.Context,
	path string,
	scope string,
) (ts oauth2.TokenSource, err error) {
	// Read the file.
	contents, err := os.ReadFile(path)
	if err != nil {
		err = fmt.Errorf("ReadFile(%q): %w", path, err)
		return
	}

	// Create a config struct based on its contents.
	jwtConfig, err := google.JWTConfigFromJSON(contents, scope)
	if err != nil {
		err = fmt.Errorf("JWTConfigFromJSON: %w", err)
		return
	}

	// Create the token source.
	ts = jwtConfig.TokenSource(ctx)

	return
}

// GetTokenSource returns a TokenSource for GCS API given a key file, or
// with the default credentials.
func GetTokenSource(
	ctx context.Context,
	keyFile string,
) (tokenSrc oauth2.TokenSource, err error) {
	// Create the oauth2 token source.
	const scope = gcs.Scope_FullControl
	var method string

	if keyFile != "" {
		tokenSrc, err = newTokenSourceFromPath(ctx, keyFile, scope)
		method = "newTokenSourceFromPath"
	} else {
		tokenSrc, err = google.DefaultTokenSource(ctx, scope)
		method = "DefaultTokenSource"
	}

	if err != nil {
		err = fmt.Errorf("%s: %w", method, err)
		return
	}
	return
}

package migration

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/payfazz/go-errors"
	migration "github.com/payfazz/psql-migration"
)

const appIDFile = "__APP_ID__.txt"

// Config .
type Config struct {
	DryRun  bool
	Dir     string
	Conn    string
	Verbose bool
}

// Run .
func Run(ctx context.Context, config Config) error {
	data, err := ioutil.ReadFile(filepath.Join(config.Dir, appIDFile))
	if os.IsNotExist(err) {
		return fmt.Errorf("cannot open file %s in %s", appIDFile, config.Dir)
	} else if err != nil {
		return errors.Wrap(err)
	}

	appID := strings.TrimSpace(string(data))

	allFiles, err := ioutil.ReadDir(config.Dir)
	if err != nil {
		return errors.Wrap(err)
	}

	var files []string

	for _, info := range allFiles {
		if info.IsDir() {
			continue
		}
		if filepath.Ext(info.Name()) == ".sql" {
			files = append(files, info.Name())
		}
	}

	var statements []string

	for _, f := range files {
		data, err = ioutil.ReadFile(filepath.Join(config.Dir, f))
		if err != nil {
			return errors.NewWithCause("cannot read file:"+f, err)
		}
		statements = append(statements, string(data))
	}

	db, err := sql.Open("postgres", config.Conn)
	if err != nil {
		return errors.NewWithCause("Cannot open database", err)
	}

	if config.DryRun {
		err = migration.DryRun(ctx, db, appID, statements)
	} else {
		err = migration.Migrate(ctx, db, appID, statements)
	}

	if err != nil {
		if err, ok := err.(*migration.InvalidAppIDError); ok {
			return fmt.Errorf(
				"application id in '"+appIDFile+"' does't match with database: %s != %s",
				appID, err.AppID,
			)
		}
		if err, ok := err.(*migration.HashError); ok {
			if err.StatementHash == "" {
				return fmt.Errorf("Error parsing '%s'", files[err.StatementIndex])
			}

			if !config.Verbose {
				return fmt.Errorf("hash for file '%s' does't match with database", files[err.StatementIndex])
			}

			return fmt.Errorf(""+
				"hash for file '%s' does't match with database\n"+
				"computed hash    : %s\n"+
				"hash on database : %s",
				files[err.StatementIndex],
				err.StatementHash,
				err.ExpectedHash,
			)
		}
		if err, ok := err.(*migration.MissingStatementError); ok {
			return fmt.Errorf("Database already applied %d, but only %d files provided", err.Needed, len(statements))
		}
		return errors.Wrap(err)
	}

	return nil
}

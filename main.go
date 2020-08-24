package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"

	"github.com/payfazz/go-errors"
	migrationcmd "github.com/payfazz/psql-migration-cmd/pkg/psql-migration"
)

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	go handleInterrupt(ctx, cancelCtx)

	dryrun, _ := strconv.ParseBool(os.Getenv("DryRun"))
	flag.BoolVar(&dryrun, "DryRun", false, "if DryRun set to true, the changes is not commited")

	dir := os.Getenv("Dir")
	flag.StringVar(&dir, "Dir", "./", "which directory contains the migration statements")

	dir, err := filepath.Abs(dir)
	if err != nil {
		crash(errors.Wrap(err))
	}

	conn := os.Getenv("Conn")
	flag.StringVar(&conn, "Conn", "", "postgres connection string")

	verbose, _ := strconv.ParseBool(os.Getenv("Verbose"))
	flag.BoolVar(&verbose, "Verbose", false, "verbose output")

	flag.Parse()

	if err := migrationcmd.Run(ctx, migrationcmd.Config{
		DryRun:  dryrun,
		Dir:     dir,
		Conn:    conn,
		Verbose: verbose,
	}); err != nil {
		crash(err)
	}

	if dryrun {
		fmt.Println("Migration complete, but not commited because of DryRun")
	} else {
		fmt.Println("Migration complete")
	}
}

func crash(err error) {
	fmt.Fprintln(os.Stderr, errors.Format(err))
	os.Exit(1)
}

func handleInterrupt(ctx context.Context, cancelCtx context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	select {
	case <-c:
	case <-ctx.Done():
	}
	signal.Stop(c)
	cancelCtx()
}

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/m-ice/NewIM/server/storage/conversationsync"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	"github.com/m-ice/NewIM/server/sync/conversation"
	"github.com/m-ice/NewIM/server/user"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:]))
}

func run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return invalidArguments()
	}
	action := args[0]
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var userID string
	limit := conversation.MaxPageItems
	switch action {
	case "step":
		flags.StringVar(&userID, "user", "", "")
		flags.IntVar(&limit, "limit", limit, "")
	case "reset":
		flags.StringVar(&userID, "user", "", "")
	case "run":
		flags.IntVar(&limit, "limit", limit, "")
	default:
		return invalidArguments()
	}
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 ||
		(action != "run" && !user.ValidID(userID)) || limit < 1 || limit > conversation.MaxPageItems {
		return invalidArguments()
	}
	readSync := os.Getenv("NEWIM_CONVERSATION_READ_SYNC")
	if readSync != "" && readSync != "0" && readSync != "1" {
		return invalidArguments()
	}
	repository, err := conversationsync.Open(ctx, conversationsync.Config{
		DSN:                os.Getenv("NEWIM_AUTH_DSN"),
		AllowLocalSocket:   os.Getenv("NEWIM_AUTH_ALLOW_LOCAL_SOCKET") == "1",
		ApplicationName:    "newim-sync-backfill",
		RequireSourceReady: true,
		IncludeReadState:   readSync == "1",
		MaxConnections:     1,
		Guard:              postgresidentity.NewGuard(nil),
	})
	if err != nil {
		return reportError(err)
	}
	defer repository.Close()

	switch action {
	case "reset":
		if err = repository.ResetSourceAccount(ctx, userID); err != nil {
			return reportError(err)
		}
		fmt.Println("SYNC_SOURCE_RESET")
	case "step":
		ready, err := repository.BackfillStep(ctx, userID, limit)
		if err != nil {
			return reportError(err)
		}
		if ready {
			fmt.Println("SYNC_SOURCE_READY")
		} else {
			fmt.Println("SYNC_SOURCE_PENDING")
		}
	case "run":
		if err = repository.RunSourceBackfill(ctx, limit); err != nil {
			return reportError(err)
		}
		fmt.Println("SYNC_SOURCE_COMPLETE")
	}
	return 0
}

func invalidArguments() int {
	fmt.Fprintln(os.Stderr, "SYNC_INVALID_ARGUMENT")
	return 2
}

func reportError(err error) int {
	fmt.Fprintln(os.Stderr, conversation.ErrorCode(err))
	return 1
}

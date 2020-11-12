package main

import (
	"fmt"
	"os"

	"github.com/iand/logfmtr"
	"github.com/urfave/cli/v2"

	"github.com/iand/gonudb/internal"
)

func main() {
	app := &cli.App{
		Name:     "gonudbadmin",
		HelpName: "gonudbadmin",
		Usage:    "Administer a gonudb store",
		Flags: []cli.Flag{
			logLevelFlag,
		},
		Version: internal.Version(),
		Commands: []*cli.Command{
			infoCommand,
			verifyCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var logLevelFlag = &cli.IntFlag{
	Name:    "log-level",
	Aliases: []string{"ll"},
	Usage:   "Set verbosity of logs to `LEVEL` (higher is more verbose)",
	Value:   0,
}

func initLogging(cc *cli.Context) error {
	logfmtr.SetVerbosity(cc.Int("log-level"))
	loggerOpts := logfmtr.DefaultOptions()
	loggerOpts.Humanize = true
	loggerOpts.Colorize = true
	logfmtr.UseOptions(loggerOpts)
	return nil
}

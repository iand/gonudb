package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/iand/logfmtr"
	"github.com/urfave/cli/v2"

	"github.com/iand/gonudb"
)

func main() {
	app := &cli.App{
		Name:      "gonudbsample",
		HelpName:  "gonudbsample",
		Usage:     "Sample application for Gonudb",
		UsageText: "gonudbsample [options] <directory>",
		ArgsUsage: "<directory>",
		Flags: []cli.Flag{
			logLevelFlag,
			concurrentFlag,
		},
		Action:          run,
		Version:         gonudb.Version(),
		HideHelpCommand: true,
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

var concurrentFlag = &cli.IntFlag{
	Name:    "concurrent",
	Aliases: []string{"c"},
	Usage:   "Perform some concurrent inserts and fetches for `SECONDS`.",
	Value:   0,
}

func run(cc *cli.Context) error {
	if cc.Args().Len() != 1 {
		return cli.Exit("Missing directory for Gonudb store", 1)
	}

	rand.Seed(time.Now().Unix())

	path := cc.Args().Get(0)

	logfmtr.SetVerbosity(cc.Int("log-level"))
	loggerOpts := logfmtr.DefaultOptions()
	loggerOpts.Humanize = true
	loggerOpts.Colorize = true
	loggerOpts.AddCaller = true
	logfmtr.UseOptions(loggerOpts)

	datPath := filepath.Join(path, "gonudb.dat")
	keyPath := filepath.Join(path, "gonudb.key")
	logPath := filepath.Join(path, "gonudb.log")

	fmt.Printf("Creating store in directory %s\n", path)
	err := gonudb.CreateStore(
		datPath,
		keyPath,
		logPath,
		1,
		gonudb.NewSalt(),
		4096,
		0.5,
	)
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && os.IsExist(pathErr) {
			fmt.Println("Store already exists")
		} else {
			return cli.Exit("Failed to create store: "+err.Error(), 1)
		}
	}

	fmt.Println("Opening store")
	s, err := gonudb.OpenStore(datPath, keyPath, logPath, &gonudb.StoreOptions{Logger: logfmtr.NewNamed("gonudb")})
	if err != nil {
		return cli.Exit("Failed to open store: "+err.Error(), 1)
	}

	defer s.Close()

	keys := make([]string, 500)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%09d", i)
	}

	fmt.Printf("Inserting %d samples\n", len(keys))
	duplicates := 0
	for i := range keys {
		if err := s.Insert(keys[i], []byte(fmt.Sprintf("this is data for %05d", i))); err != nil {
			if errors.Is(err, gonudb.ErrKeyExists) {
				duplicates++
				continue
			}
			return cli.Exit("Failed to insert: "+err.Error(), 1)
		}
	}
	fmt.Printf("Skipped %d duplicates\n", duplicates)

	fmt.Println("Finding random keys")
	for i := 0; i < len(keys)/25; i++ {
		key := keys[rand.Intn(len(keys))]
		if err := s.Fetch(key, func(data []byte) {
			fmt.Printf("Found %s => %s\n", key, string(data))
		}); err != nil {
			return cli.Exit("Failed to fetch "+key+": "+err.Error(), 1)
		}
	}

	if cc.Int("concurrent") == 0 {
		return nil
	}
	fmt.Println("Running some concurrent inserts and fetches")
	ctx, cancel := context.WithTimeout(cc.Context, time.Duration(cc.Int("concurrent"))*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(7)

	for i := 0; i < 2; i++ {
		go func(ctx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				for i := 0; i < 500; i++ {
					key := fmt.Sprintf("%08d", rand.Intn(10000000))
					data := fmt.Sprintf("this is data for %s", key)
					if err := s.Insert(key, []byte(data)); err != nil && !errors.Is(err, gonudb.ErrKeyExists) {
						fmt.Printf("Failed to insert: %v\n", err)
						return
					}
				}
				fmt.Println("Wrote 500 records")
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}(ctx, &wg)
	}

	for i := 0; i < 5; i++ {
		go func(tx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				for i := 0; i < 500; i++ {
					key := fmt.Sprintf("%08d", rand.Intn(10000000))
					err := s.Fetch(key, func(data []byte) {})
					if err != nil && !errors.Is(err, gonudb.ErrKeyNotFound) {
						fmt.Printf("Failed to fetch: %v\n", err)
						return
					}
				}
				fmt.Println("Read 500 records")
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}(ctx, &wg)
	}

	wg.Wait()

	return nil
}

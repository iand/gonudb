package main

import (
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/iand/gonudb/internal"
)

var infoCommand = &cli.Command{
	Name:      "info",
	Usage:     "Report information about one or more gonudb files.",
	ArgsUsage: "<file>...",
	Action:    info,
	Flags: []cli.Flag{
		logLevelFlag,
	},
}

func info(cc *cli.Context) error {
	if err := initLogging(cc); err != nil {
		return cli.Exit(err.Error(), 1)
	}

	if cc.Args().Len() == 0 {
		cli.ShowAppHelpAndExit(cc, 1)
	}

	for i := 0; i < cc.Args().Len(); i++ {
		path := cc.Args().Get(i)
		Print(os.Stdout, &section{
			label: path,
			rows:  infoFile(path),
		})
	}

	return nil
}

func infoFile(path string) []kv {
	f, err := os.Open(path)
	if err != nil {
		return []kv{{Key: "Error", Value: fmt.Errorf("failed to open file: %w", err)}}
	}
	defer f.Close()

	fStat, err := f.Stat()
	if err != nil {
		return []kv{{Key: "Error", Value: fmt.Errorf("failed to stat file: %w", err)}}
	}

	var typeHeader [8]byte

	if _, err := f.ReadAt(typeHeader[:], 0); err != nil {
		return []kv{{Key: "Error", Value: fmt.Errorf("failed to read file type: %w", err)}}
	}

	switch string(typeHeader[:]) {
	case string(internal.DatFileHeaderType):
		var dh internal.DatFileHeader
		if err := dh.DecodeFrom(f); err != nil {
			return []kv{{Key: "Error", Value: fmt.Errorf("failed to read data file header: %w", err)}}
		}

		return []kv{
			{Key: "Type", Value: string(dh.Type[:])},
			{Key: "Version", Value: dh.Version},
			{Key: "UID", Value: dh.UID},
			{Key: "AppNum", Value: dh.AppNum},
			{Key: "File size", Value: Bytes(fStat.Size())},
		}
	case string(internal.KeyFileHeaderType):
		var kh internal.KeyFileHeader
		if err := kh.DecodeFrom(f, fStat.Size()); err != nil {
			return []kv{{Key: "Error", Value: fmt.Errorf("failed to read key file header: %w", err)}}
		}

		return []kv{
			{Key: "Type", Value: string(kh.Type[:])},
			{Key: "Version", Value: kh.Version},
			{Key: "UID", Value: kh.UID},
			{Key: "AppNum", Value: kh.AppNum},
			{Key: "Salt", Value: kh.Salt},
			{Key: "Pepper", Value: kh.Pepper},
			{Key: "BlockSize", Value: Bytes(kh.BlockSize)},
			{Key: "Capacity", Value: kh.Capacity},
			{Key: "Buckets", Value: kh.Buckets},
			{Key: "Modulus", Value: kh.Modulus},
			{Key: "File size", Value: Bytes(fStat.Size())},
		}
	case string(internal.LogFileHeaderType):
		var lh internal.LogFileHeader
		if err := lh.DecodeFrom(f); err != nil {
			return []kv{{Key: "Error", Value: fmt.Errorf("failed to read log file header: %w", err)}}
		}

		return []kv{
			{Key: "Type", Value: string(lh.Type[:])},
			{Key: "Version", Value: lh.Version},
			{Key: "UID", Value: lh.UID},
			{Key: "AppNum", Value: lh.AppNum},
			{Key: "Salt", Value: lh.Salt},
			{Key: "Pepper", Value: lh.Pepper},
			{Key: "BlockSize", Value: Bytes(lh.BlockSize)},
			{Key: "KeyFileSize", Value: Bytes(lh.KeyFileSize)},
			{Key: "DatFileSize", Value: Bytes(lh.DatFileSize)},
			{Key: "File size", Value: Bytes(fStat.Size())},
		}
	default:
		return []kv{{Key: "Error", Value: fmt.Sprintf("unknown file type: %s", string(typeHeader[:]))}}
	}
}

type section struct {
	label string
	rows  []kv
}

type kv struct {
	Key   string
	Value interface{}
}

func Print(w io.Writer, s *section) {
	fmt.Fprintln(w, s.label)
	maxKeyLen := 0
	for _, r := range s.rows {
		if len(r.Key) > maxKeyLen {
			maxKeyLen = len(r.Key)
		}
	}

	fmtstr := fmt.Sprintf("  %%-%ds: %%v\n", maxKeyLen)
	for _, r := range s.rows {
		fmt.Fprintf(w, fmtstr, r.Key, r.Value)
	}
	fmt.Fprintln(w)
}

type Bytes int64

func (b Bytes) String() string {
	return fmt.Sprintf("%d bytes", b)
}

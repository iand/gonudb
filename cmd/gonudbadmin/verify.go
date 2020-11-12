package main

import (
	"os"

	"github.com/iand/gonudb/internal"
	"github.com/urfave/cli/v2"
)

var verifyCommand = &cli.Command{
	Name:      "verify",
	Usage:     "Verify consistency of data and key files.",
	ArgsUsage: "<datafile> <keyfile>",
	Action:    verify,
	Description: "" +
		"Verifies the consistency and integrity of a store by analysing its data and\n" +
		"key files. Reports statistical information about the data distribution and\n" +
		"efficiency of the store.",
	Flags: []cli.Flag{
		logLevelFlag,
	},
}

func verify(cc *cli.Context) error {
	if err := initLogging(cc); err != nil {
		return cli.Exit(err.Error(), 1)
	}

	if cc.Args().Len() == 0 {
		cli.ShowAppHelpAndExit(cc, 1)
	}

	if cc.Args().Len() != 2 {
		return cli.Exit("expecting paths to data and key files", 1)
	}
	datPath := cc.Args().Get(0)
	keyPath := cc.Args().Get(1)

	info, err := internal.VerifyStore(datPath, keyPath)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}

	Print(os.Stdout, &section{
		label: "Store metadata",
		rows: []kv{
			{Key: "Version", Value: info.Version},
			{Key: "UID", Value: info.UID},
			{Key: "AppNum", Value: info.AppNum},
		},
	})

	Print(os.Stdout, &section{
		label: "Data file",
		rows: []kv{
			{Key: "DatFileSize", Value: Bytes(info.DatFileSize)},
			{Key: "ValueCountInUse", Value: info.ValueCountInUse},
			{Key: "ValueCountTotal", Value: info.ValueCountTotal},
			{Key: "ValueBytesInUse", Value: Bytes(info.ValueBytesInUse)},
			{Key: "ValueBytesTotal", Value: Bytes(info.ValueBytesTotal)},
			{Key: "SpillCountInUse", Value: info.SpillCountInUse},
			{Key: "SpillCountTotal", Value: info.SpillCountTotal},
			{Key: "SpillBytesInUse", Value: Bytes(info.SpillBytesInUse)},
			{Key: "SpillBytesTotal", Value: Bytes(info.SpillBytesTotal)},
			{Key: "AverageFetch", Value: info.AverageFetch},
			{Key: "Waste", Value: info.Waste},
			{Key: "Overhead", Value: info.Overhead},
			{Key: "ActualLoad", Value: info.ActualLoad},
		},
	})

	Print(os.Stdout, &section{
		label: "Key file",
		rows: []kv{
			{Key: "KeyFileSize", Value: Bytes(info.KeyFileSize)},
			{Key: "KeySize", Value: Bytes(info.KeySize)},
			{Key: "Salt", Value: info.Salt},
			{Key: "Pepper", Value: info.Pepper},
			{Key: "BlockSize", Value: Bytes(info.BlockSize)},
			{Key: "LoadFactor", Value: info.LoadFactor},
			{Key: "Capacity", Value: info.Capacity},
			{Key: "Buckets", Value: info.Buckets},
			{Key: "BucketSize", Value: info.BucketSize},
			{Key: "Modulus", Value: info.Modulus},
			{Key: "KeyCount", Value: info.KeyCount},
		},
	})

	return nil
}

package main

import (
	"github.com/mongodb/mongo-tools/bsondump"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/common/util"
	"os"
)

func GetOplogHeadTailTimestamp(filepath string) ([]bsondump.OplogTimestamp, error) {
	// initialize command-line opts
	opts := options.New("bsondump", bsondump.Usage, options.EnabledOptions{})
	bsonDumpOpts := &bsondump.BSONDumpOptions{
		BSONFileName: filepath,
	}
	opts.AddOptions(bsonDumpOpts)

	log.SetVerbosity(opts.Verbosity)
	signals.Handle()

	dumper := bsondump.BSONDump{
		ToolOptions:     opts,
		BSONDumpOptions: bsonDumpOpts,
	}

	reader, err := bsonDumpOpts.GetBSONReader()
	if err != nil {
		log.Logvf(log.Always, "Getting BSON Reader Failed: %v", err)
		return nil, err
	}
	dumper.BSONSource = db.NewBSONSource(reader)
	defer dumper.BSONSource.Close()

	if len(bsonDumpOpts.Type) != 0 && bsonDumpOpts.Type != "debug" && bsonDumpOpts.Type != "json" {
		log.Logvf(log.Always, "Unsupported output type '%v'. Must be either 'debug' or 'json'", bsonDumpOpts.Type)
		os.Exit(util.ExitBadOptions)
	}

	return dumper.OplogHeadTailJSON()
}

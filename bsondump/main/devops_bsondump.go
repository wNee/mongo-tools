package mongotools

import (
	"os"

	"github.com/wNee/mongo-tools/bsondump"
	"github.com/wNee/mongo-tools/common/db"
	"github.com/wNee/mongo-tools/common/log"
	"github.com/wNee/mongo-tools/common/options"
	"github.com/wNee/mongo-tools/common/signals"
	"github.com/wNee/mongo-tools/common/util"
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

	return dumper.HeadTailTimestamp()
}

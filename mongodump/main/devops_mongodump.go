package main

import (
	"fmt"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/mongodump"
)

const (
	devops_progressBarLength   = 24
	devops_progressBarWaitTime = time.Second * 3
)

func main() {
	cmd := DefaultDevopsMongodumpCommand()
	cmd.Port = "27009"
	cmd.DB = "local"
	cmd.Collection = "oplog.rs"
	DevopsMongodump(cmd)

}
type DevopsMongodumpCommand struct {
	Host string  `json:"host"`
	Port string     `json:"port"`
	User string  `json:"user"`
	Pwd  string  `json:"pwd"`
	DB   string  `json:"db"`
	Collection string `json:"collection"`
	Query  string  `json:"query"`
	Gzip   bool    `json:"gzip"`
	Output    string   `json:"output"`
	Oplog  bool    `json:"oplog"`
	ExcludeCollection  []string   `json:"exclude_collection"`
	AuthenticationDatabase string   `json:"authentication_database"`
}

func DefaultDevopsMongodumpCommand() DevopsMongodumpCommand {
	return DevopsMongodumpCommand{
		Host:              "127.0.0.1",
		Port:              "27017",
		User:              "",
		Pwd:               "",
		DB:                "",
		Collection:        "",
		Query:             "",
		Gzip:              false,
		Output:               "",
		Oplog:             false,
		ExcludeCollection: nil,
	}
}

//func devopsMongoDumpCmdValidator(cmd *DevopsMongodumpCommand) error {
//	if cmd.Collection != "" && cmd.DB == "" {
//		return errors.New("need database when use collection")
//	}
//	if cmd.Collection != "" && cmd.ExcludeCollection != nil {
//		return errors.New(" --collection is not allowed when --excludeCollection is specified")
//	}
//	return nil
//}
func convertToOption(cmd *DevopsMongodumpCommand) []string {
	result := []string{}
	if cmd.Host != "" {
		result = append(result, "--host")
		result = append(result, cmd.Host)
	}
	if cmd.Port != "" {
		result = append(result, "--port")
		result = append(result, cmd.Port)
	}
	if cmd.User != "" {
		result = append(result, "--username")
		result = append(result, cmd.User)
	}
	if cmd.Pwd != "" {
		result = append(result, "--password")
		result = append(result, cmd.Pwd)
	}
	if cmd.AuthenticationDatabase != "" {
		result = append(result, "--authenticationDatabase")
		result = append(result, cmd.AuthenticationDatabase)
	}
	if cmd.DB != "" {
		result = append(result, "--db")
		result = append(result, cmd.DB)
	}
	if cmd.Output != "" {
		result = append(result, "--output")
		result = append(result, cmd.Output)
	}
	if cmd.Collection != "" {
		result = append(result, "--collection")
		result = append(result, cmd.Collection)
	}

	if cmd.ExcludeCollection != nil {
		for _, v := range cmd.ExcludeCollection {
			result = append(result, "--excludeCollection")
			result = append(result, v)
		}
	}
	if cmd.Gzip {
		result = append(result, "--gzip")
	}
	if cmd.Oplog {
		result = append(result, "--oplog")
	}
	if cmd.Query != "" {
		result = append(result, "--query")
		result = append(result, cmd.Query)
	}
	return result
}
func DevopsMongodump(cmd DevopsMongodumpCommand) error {
	// initialize command-line opts
	opts := options.New("mongodump", mongodump.Usage, options.EnabledOptions{Auth: true, Connection: true, Namespace: true, URI: true})

	inputOpts := &mongodump.InputOptions{}
	opts.AddOptions(inputOpts)
	outputOpts := &mongodump.OutputOptions{}
	opts.AddOptions(outputOpts)
	opts.URI.AddKnownURIParameters(options.KnownURIOptionsReadPreference)
	args := convertToOption(&cmd)
	pargs, err := opts.ParseArgs(args)
	if err != nil {
		return err
	}
	if len(pargs) > 0 {
		return fmt.Errorf("positional arguments not allowed: %v", pargs)
	}

	// verify uri options and log them
	opts.URI.LogUnsupportedOptions()

	// kick off the progress bar manager
	progressManager := progress.NewBarWriter(log.Writer(0), devops_progressBarWaitTime, devops_progressBarLength, false)
	progressManager.Start()
	defer progressManager.Stop()

	dump := mongodump.MongoDump{
		ToolOptions:     opts,
		OutputOptions:   outputOpts,
		InputOptions:    inputOpts,
		ProgressManager: progressManager,
	}

	finishedChan := signals.HandleWithInterrupt(dump.HandleInterrupt)
	defer close(finishedChan)

	if err = dump.Init(); err != nil {
		return err
	}

	if err = dump.Dump(); err != nil {
		return err
	}
	return nil
}

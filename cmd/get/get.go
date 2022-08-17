package get

import (
	"github.com/CorentinB/Zeno/cmd"
	"github.com/CorentinB/Zeno/config"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func initLogging(c *cli.Context) (err error) {
	// Log as JSON instead of the default ASCII formatter.
	if config.App.Flags.JSON {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	// Turn on debug mode
	if config.App.Flags.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	return nil
}

func init() {
	cmd.RegisterCommand(
		cli.Command{
			Name:  "get",
			Usage: "Archive the web!",
			Subcommands: []*cli.Command{
				newGetURLCmd(),
				newGetListCmd(),
				newGetHQCmd(),
			},
		})
}

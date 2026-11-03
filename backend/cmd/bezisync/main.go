// Command bezisync is the BeziSync control plane: the api, controller and agent roles,
// database migrations, administration commands and the `remote` client.
package main

import (
	"os"

	"github.com/daun-gatal/bezisync/backend/internal/cli"
)

func main() {
	os.Exit(cli.Execute(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

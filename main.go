package main

import (
	"github.com/strategicpause/slashie"
	"weedupe/director"
)

func main() {
	s := slashie.NewSlashie()

	d := director.NewDirector(
		director.WithSlashie(s),
		director.WithFile("./data/foo.txt"),
		director.WithFile("./data/bar.txt"),
		director.WithFile("./data/test.txt"),
	)
	d.Start()
}

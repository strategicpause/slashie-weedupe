package main

import (
	"fmt"
	"github.com/strategicpause/slashie"
	"weedupe/director"
)

func main() {
	s := slashie.NewSlashie()

	d, err := director.NewDirector(
		director.WithSlashie(s),
		director.WithFile("./data/foo.txt"),
		director.WithFile("./data/bar.txt"),
		director.WithFile("./data/test.txt"),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	d.Wait()
}

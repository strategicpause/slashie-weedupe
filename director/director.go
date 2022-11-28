package director

import (
	"fmt"
	"github.com/strategicpause/slashie"
	"github.com/strategicpause/slashie/actor"
	"github.com/strategicpause/slashie/logger"
	"github.com/strategicpause/slashie/transition"
	"weedupe/mapper"
)

const (
	ActorType = "Director"
	ActorId   = "Main"

	InitStatus        = "Init"
	MapReduceStatus   = "MapReduce"
	ReadResultsStatus = "ReadResults"
	CombineStatus     = "Combine"
	PrintStatus       = "Print"
)

type Opt func(d *Director)

func WithFile(file string) Opt {
	return func(d *Director) {
		d.files = append(d.files, file)
	}
}

func WithSlashie(s slashie.Slashie) Opt {
	return func(d *Director) {
		d.slashie = s
	}
}

type Director struct {
	*actor.BasicActor
	slashie         slashie.Slashie
	files           []string
	wordCountByFile map[string]map[string]int
	wordCounts      map[string]int
	logger          logger.Logger
}

func NewDirector(opts ...Opt) (*Director, error) {
	d := &Director{
		BasicActor:      actor.NewBasicActor(ActorType, ActorId),
		wordCountByFile: map[string]map[string]int{},
		wordCounts:      map[string]int{},
		logger:          logger.NewStdOutLogger(),
	}

	for _, opt := range opts {
		opt(d)
	}

	if d.slashie == nil {
		d.slashie = slashie.NewSlashie()
	}

	if err := d.initActor(); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *Director) initActor() error {
	d.slashie.AddActor(d, InitStatus, PrintStatus)

	err := d.slashie.AddTransitionActions(d, []*transition.TransitionAction{
		{SrcStatus: InitStatus, DestStatus: MapReduceStatus, Action: d.MapReduce},
		{SrcStatus: ReadResultsStatus, DestStatus: CombineStatus, Action: d.Combine},
		{SrcStatus: CombineStatus, DestStatus: PrintStatus, Action: d.Print},
	})
	if err != nil {
		return err
	}

	return d.slashie.UpdateStatus(d, MapReduceStatus)
}

func (d *Director) MapReduce() error {
	for _, file := range d.files {
		if err := d.createMapReducer(file); err != nil {
			return err
		}
	}

	return d.slashie.UpdateStatus(d, ReadResultsStatus)
}

func (d *Director) createMapReducer(file string) error {
	m := mapper.NewMapper(file, d.slashie)
	// This indicates that the Director can't transition to the Combine status until the MapReducer
	// has transitioned to the Reduce status.
	err := d.slashie.AddTransitionDependency(d, ReadResultsStatus, m, mapper.ReduceStatus)
	if err != nil {
		return err
	}

	err = d.slashie.AddTransitionAction(d, MapReduceStatus, ReadResultsStatus, func() error {
		d.wordCountByFile[file] = m.GetWordCounts()

		return d.slashie.UpdateStatus(d, CombineStatus)
	})
	if err != nil {
		return err
	}

	return m.Start()
}

func (d *Director) Combine() error {
	for _, wordCounts := range d.wordCountByFile {
		for word, count := range wordCounts {
			d.wordCounts[word] += count
		}
	}

	return d.slashie.UpdateStatus(d, PrintStatus)
}

func (d *Director) Print() error {
	for word, count := range d.wordCounts {
		fmt.Printf("%s: %d\n", word, count)
	}

	return nil
}

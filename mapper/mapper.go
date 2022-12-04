package mapper

import (
	"github.com/strategicpause/slashie"
	"github.com/strategicpause/slashie/actor"
	"github.com/strategicpause/slashie/logger"
	"github.com/strategicpause/slashie/transition"
	"os"
	"regexp"
	"strings"
)

const (
	ActorType = "MapReduce"

	InitStatus   = "Init"
	ReadStatus   = "Read"
	MapStatus    = "Map"
	ReduceStatus = "Reduce"
)

type WordCounts map[string]int

var WordCountType = WordCounts{}

type Pair struct {
	A string
	B int
}

type MapReduce struct {
	*actor.BasicActor
	parentKey  actor.Key
	logger     logger.Logger
	file       string
	content    string
	splitRe    *regexp.Regexp
	replRe     *regexp.Regexp
	pairs      []*Pair
	wordCounts WordCounts
	s          slashie.Slashie
}

func NewMapper(fileName string, parentKey actor.Key, s slashie.Slashie) *MapReduce {
	mr := &MapReduce{
		BasicActor: actor.NewBasicActor(ActorType, actor.Id(fileName)),
		parentKey:  parentKey,
		logger:     logger.NewStdOutLogger(),
		splitRe:    regexp.MustCompile(`\s+`),
		replRe:     regexp.MustCompile(`\W+`),
		file:       fileName,
		pairs:      []*Pair{},
		wordCounts: WordCounts{},
		s:          s,
	}

	s.AddActor(mr, InitStatus, ReduceStatus)

	return mr
}

func (m *MapReduce) Start() error {
	err := m.s.AddTransitionActions(m, []*transition.TransitionAction{
		{SrcStatus: InitStatus, DestStatus: ReadStatus, Action: m.readData},
		{SrcStatus: ReadStatus, DestStatus: MapStatus, Action: m.mapData},
		{SrcStatus: MapStatus, DestStatus: ReduceStatus, Action: m.reduceData},
	})
	if err != nil {
		return err
	}

	return m.s.UpdateStatus(m, ReadStatus)
}

func (m *MapReduce) readData() error {
	content, err := os.ReadFile(m.file)
	if err != nil {
		return err
	}
	m.content = string(content)

	return m.s.UpdateStatus(m, MapStatus)
}

func (m *MapReduce) mapData() error {
	words := m.splitRe.Split(m.content, -1)
	for _, word := range words {
		word = m.replRe.ReplaceAllString(word, "")
		word = strings.TrimSpace(word)
		if word != "" {
			m.pairs = append(m.pairs, &Pair{A: word, B: 1})
		}
	}

	return m.s.UpdateStatus(m, ReduceStatus)
}

func (m *MapReduce) reduceData() error {
	for _, pair := range m.pairs {
		m.wordCounts[pair.A] += pair.B
	}

	return m.s.SendMessage(m.parentKey, m.wordCounts)
}

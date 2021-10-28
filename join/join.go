package join

import (
	"bufio"
	"container/heap"
	"log"
	"os"
	"regexp"
	"sync"
	"time"
)

var wg sync.WaitGroup
var timeLayout = "02-01-2006 15:04:05.000"
var dateReg *regexp.Regexp = regexp.MustCompile(`^(\d{2}-\d{2}-\d{4}\s\d{2}:\d{2}:\d{2}\.\d{3})`)

func process(logs map[string]chan interface{}, lines chan *Line, output chan string) {
	pq := NewHeap()
	cnt := 0
	N := len(logs)
	var cur *Line

	for line := range lines {
		cnt++
		if line != nil {
			heap.Push(pq, line)
		}
		if cnt >= N && len(*pq) > 0 {
			cur = heap.Pop(pq).(*Line)
			logs[cur.file] <- nil
			output <- cur.line
		}
	}
	// take the remaining line from heap
	for len(*pq) > 0 {
		cur = heap.Pop(pq).(*Line)
		output <- cur.line
	}
	close(output)
}

func Join(files []string, output chan string, verbose bool) {
	N := len(files)
	wg.Add(N)
	lines := make(chan *Line, 100)
	logs := make(map[string]chan interface{})

	for _, file := range files {
		logs[file] = make(chan interface{})
		go read(file, lines, logs[file], wg, verbose)
	}

	go process(logs, lines, output)
	wg.Wait()
	close(lines)
}

func read(file string, lines chan *Line, ready chan interface{}, done sync.WaitGroup, verbose bool) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var text string
	var t time.Time
	var matches []string
	for scanner.Scan() {
		text = scanner.Text()
		matches = dateReg.FindStringSubmatch(text)
		if len(matches) == 0 {
			continue
		}
		t, _ = time.Parse(timeLayout, matches[0])
		if verbose {
			text = file + ":::" + text
		}
		lines <- &Line{line: text, file: file, ts: t}
		<-ready
	}
	lines <- nil
	wg.Done()
}

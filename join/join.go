package join

import (
	"bufio"
	"container/heap"
	"log"
	"os"
	"sync"
	"time"
)

var wg sync.WaitGroup

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
}

func Join(files []string, output chan string) {
	N := len(files)
	wg.Add(N)
	lines := make(chan *Line, 100)
	logs := make(map[string]chan interface{})

	for _, file := range files {
		logs[file] = make(chan interface{})
		go read(file, lines, logs[file], wg)
	}

	go process(logs, lines, output)
	wg.Wait()
	close(lines)
	close(output)
}

func read(file string, lines chan *Line, ready chan interface{}, done sync.WaitGroup) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines <- &Line{line: scanner.Text(), file: file, ts: time.Now()}
		// wait until the channel is ready
		<-ready
	}
	lines <- nil
	wg.Done()
}

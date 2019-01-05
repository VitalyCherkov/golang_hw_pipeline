package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// GetCrc32 возвращает канал, из которого придет ответ
func GetCrc32(data string) chan string {
	result := make(chan string, 1)

	go func(out chan<- string){
		out <- DataSignerCrc32(data)
	}(result)

	return result
}

// SingleHashSlave выполняет хэширование для текущего числа из первоночального канала in
func SingleHashSlave(data string, out chan interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()

	crc32Chan1 := GetCrc32(data)

	mu.Lock()
	md5data := DataSignerMd5(data)
	mu.Unlock()

	crc32Chan2 := GetCrc32(md5data)

	left := <- crc32Chan1
	right := <- crc32Chan2

	out <- left + "~" + right
}

// SingleHash хэширует все числа с помощью SingleHashSlave
func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for data := range in {
		stringData := fmt.Sprintf("%v", data)

		wg.Add(1)
		go SingleHashSlave(stringData, out, wg, mu)
	}

	wg.Wait()
}

// MultiHashSlave делает мультихэширование для одного элемента из канала in
func MultiHashSlave(data string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	innerWg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	results := [6]string{}

	for i := 0; i < 6; i++ {
		innerWg.Add(1)
		go func(index int, data string, wg *sync.WaitGroup, mu *sync.Mutex){
			defer wg.Done()

			res := DataSignerCrc32(data)
			mu.Lock()
			results[index] = res
			mu.Unlock()
		}(i, fmt.Sprintf("%d%s", i, data), innerWg, mu)
	}

	innerWg.Wait()
	res := strings.Join(results[:], "")
	out <- res
}

// MultiHash хэширует все числа с помощью MultiHashSlave
func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		stringData := fmt.Sprintf("%v", data)
		wg.Add(1)
		go MultiHashSlave(stringData, out, wg)
	}
	wg.Wait()
}

// CombineResults комбинирует все захэшированные и отправляет в канал ответа
func CombineResults(in, out chan interface{}) {
	var results []string
	for data := range in {
		results = append(results, data.(string))
	}
	sort.Strings(results)
	answer := strings.Join(results, "_")
	out <- answer
}

// PipelineWrapper оборачивает текущий элемент pipeline-а для корректной работы WaitGroup и закрытия каналов
func PipelineWrapper(job job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	job(in, out)
	close(out)
}

// ExecutePipeline - запускает каждый из элементов пайплайна
func ExecutePipeline(jobs ...job) {
	var prevOut chan interface{}
	wg := &sync.WaitGroup{}

	for _, job := range jobs {
		curChan := make(chan interface{}, MaxInputDataLen)
		wg.Add(1)
		go PipelineWrapper(job, prevOut, curChan, wg)
		prevOut = curChan
	}

	wg.Wait()
}

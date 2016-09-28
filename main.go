package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"
)

var (
	iodepth    = flag.Int("iodepth", 1, "IO que depth")
	cpus       = flag.Int("cpus", 2, "IO que depth")
	indir      = flag.String("in", "", "Source folder")
	outdir     = flag.String("out", "", "Output folder")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	numfiles   = 0
)

const (
	MB_B = 1.0 / (1024.0 * 1024.0)
)

func copy(fnames []os.FileInfo, id int, done chan float64) {
	totalsize := float64(0.0)
	buffer := make([]byte, 4*1024*1024)
	idx := id
	for {
		if idx < numfiles {

			file := fnames[idx]
			idx += (*iodepth)

			filename := file.Name()

			name_in := path.Join(*indir, filename)
			name_out := path.Join(*outdir, name_in)

			fin, err := os.OpenFile(name_in, os.O_RDONLY, 0)
			if err != nil {
				log.Println(err)
			}

			stat, err := fin.Stat()
			if err != nil {
				log.Println(err)
			}

			if stat.IsDir() {
				fin.Close()
				continue
			}

			fout, err := os.OpenFile(name_out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, stat.Mode())
			if err != nil {
				log.Println(err)
			}

			size, err := io.CopyBuffer(fout, fin, buffer)
			if err != nil {
				log.Println(err)
			}

			fin.Close()
			fout.Close()
			totalsize += (MB_B * float64(size))
		} else {
			done <- totalsize
			return
		}

	}
}

func main() {

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	runtime.GOMAXPROCS(*cpus)

	stat, err := os.Stat(*indir)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Getting file list...")
	files, err := ioutil.ReadDir(*indir)
	if err != nil {
		log.Fatal(err)
	}

	numfiles = len(files)

	log.Println("Creating output folder...")
	err = os.Mkdir(path.Join(*outdir, path.Base(*indir)), stat.Mode())
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan float64)

	debug.SetGCPercent(-1)

	start := time.Now()

	for i := 0; i < *iodepth; i++ {
		go copy(files, i, done)
	}

	totalsize := float64(0.0)

	for i := 0; i < *iodepth; i++ {
		totalsize += <-done
	}

	elapsed := time.Since(start)

	debug.SetGCPercent(100)

	throughput := totalsize / elapsed.Seconds()
	log.Printf("Copied %.2f MB @ %.2f MB/s\n", totalsize, throughput)

}

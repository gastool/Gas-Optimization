package util

import (
	"encoding/csv"
	"io"
	"os"
	"strings"
)

func ReadCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	PanicOnError(err)
	defer func() {
		err := f.Close()
		PanicOnError(err)
	}()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	PanicOnError(err)

	return records[1:]  // ignore first line
}

func ReadCsvFileByLine(filePath string, onNext func(record []string), onComplete func()) {
	f, err := os.Open(filePath)
	PanicOnError(err)
	defer func() {
		err := f.Close()
		PanicOnError(err)
	}()

	csvReader := csv.NewReader(f)
	_, firstLineErr := csvReader.Read()  // ignore first line
	PanicOnError(firstLineErr)
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				if onComplete != nil {
					onComplete()
				}
				break
			}
			PanicOnError(err)
		}
		if onNext != nil {
			onNext(record)
		}
	}
}

// ReadCsvFileByLineWithSkip skipLines does not include header line
func ReadCsvFileByLineWithSkip(filePath string, skipLines int, onFullySkipped func(), onNext func(record []string), onComplete func()) {
	f, err := os.Open(filePath)
	PanicOnError(err)
	defer func() {
		err := f.Close()
		PanicOnError(err)
	}()

	csvReader := csv.NewReader(f)
	_, firstLineErr := csvReader.Read()  // ignore first line
	PanicOnError(firstLineErr)
	fullySkipped := false
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				if onComplete != nil {
					onComplete()
				}
				break
			}
			PanicOnError(err)
		}
		if skipLines > 0 {
			skipLines -= 1
		} else {
			if !fullySkipped && onFullySkipped != nil {
				onFullySkipped()
				fullySkipped = true
			}
			if onNext != nil {
				onNext(record)
			}
		}
	}
}

func WriteCsvFileByLine(file *os.File, data []string) {
	lockByName(file.Name())
	defer unlockByName(file.Name())

	if len(data) == 0 {
		return
	}
	var builder strings.Builder
	first := true
	for _, item := range data {
		if first {
			first = false
		} else {
			builder.WriteString(",")
		}
		if len(item) > 0 {
			builder.WriteString(escapeCsvString(item))
		}
	}
	builder.WriteString("\n")
	writeFile(file, builder.String())
}

func escapeCsvString(string string) string {
	if len(string) == 0 {
		return string
	}
	needToQuote := false
	for _, char := range string {
		if char == '"' || char == ',' {
			needToQuote = true
			break
		}
	}
	if needToQuote {
		return "\"" + strings.Replace(string, "\"", "\"\"", -1) + "\""
	} else {
		return string
	}
}

func writeFile(file *os.File, string string) {
	_, err := file.WriteString(string)
	PanicOnError(err)
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func OpenCsvFileOrCreateWithHeaders(filename string, headers []string) *os.File {
	file, openFileErr := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	PanicOnError(openFileErr)
	stat, statErr := file.Stat()
	PanicOnError(statErr)
	if stat.Size() == 0 {
		_, err := file.WriteString(strings.Join(headers, ",") + "\n")
		PanicOnError(err)
	}
	return file
}

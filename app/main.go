package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	switch command {
	case ".dbinfo":
		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v", pageSize)

		// Printing number of tables
		ph, err := extractPageHeader(databaseFile)
		if err != nil {
			log.Fatalf("Failed to extract nCells %v", err)
		}
		fmt.Printf("\nnumber of tables: %d", ph.nCells())
	case ".tables":
		cellPArr, err := cellPointerArray(databaseFile)
		if err != nil {
			log.Fatalf("failed to read cell pointer array. %v", err)
		}
		// fmt.Printf("\ncellPArr %v", cellPArr)
		var tables []string
		for _, ptr := range cellPArr {
			if ptr == 0 {
				continue
			}
			c, err := extractCell(int64(ptr), databaseFile)
			if err != nil {
				log.Fatalf("Failed to load cell at %v. %v", ptr, err)
			}
			r := c.record()
			tables = append(tables, r.tableName())
		}
		fmt.Printf("%s", strings.Join(tables, " "))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func cellPointerArray(f *os.File) ([]uint16, error) {
	ph, err := extractPageHeader(f)
	if err != nil {
		return nil, err
	}
	// fmt.Printf("\nph %v\n", ph)
	k := ph.nCells()

	// fmt.Printf("\nthe start of the cell content area %v\n", ph.startCellPtr())

	cellPointerArray := make([]byte, k*2)
	_, err = f.ReadAt(cellPointerArray, 108)
	if err != nil {
		return nil, err
	}
	// fmt.Printf("cellPointerArray %v", cellPointerArray)

	pArr := make([]uint16, k)

	for i := uint16(0); i < k; i++ {
		pArr[i] = binary.BigEndian.Uint16(cellPointerArray[i*2 : i*2+2])
	}

	// fmt.Printf("\npArr %v\n", pArr)

	return pArr, nil
}

type PageHeader []byte

// nCells retuern number of cells from page header
func (p PageHeader) nCells() uint16 {
	return binary.BigEndian.Uint16(p[3:5])
}

func (p PageHeader) startCellPtr() uint16 {
	return binary.BigEndian.Uint16(p[5:7])
}

func extractPageHeader(f *os.File) (*PageHeader, error) {
	header := make([]byte, 12)
	_, err := f.ReadAt(header, 100)
	if err != nil {
		return nil, err
	}

	return (*PageHeader)(&header), nil
}

func extractCell(ptr int64, f *os.File) (Cell, error) {
	cellSize := make([]byte, 1)
	_, err := f.ReadAt(cellSize, ptr)
	if err != nil {
		return nil, err
	}
	cell := make([]byte, cellSize[0])

	_, err = f.ReadAt(cell, ptr)
	if err != nil {
		return nil, err
	}

	return (Cell)(cell), nil
}

type Cell []byte
type Record []byte

func (c Cell) size() uint16 {
	return binary.BigEndian.Uint16(c[0:1])
}

func (c Cell) rowID() uint16 {
	return binary.BigEndian.Uint16(c[1:2])
}

func (c Cell) record() Record {
	return (Record)(c[2:])
}

func (r Record) headerSize() uint16 {
	return uint16(r[0])
}

func (r Record) typeSize() uint16 {
	return (uint16(r[1]) - 13) / 2
}

func (r Record) nameSize() uint16 {
	return (uint16(r[2]) - 13) / 2
}

func (r Record) tableNameSize() uint16 {
	return (uint16(r[3]) - 13) / 2
}

func (r Record) tableName() string {
	startPtr := r.headerSize() + r.typeSize() + r.nameSize()
	return string(r[startPtr : startPtr+r.tableNameSize()])
}

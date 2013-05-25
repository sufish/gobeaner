/**
 * Created with IntelliJ IDEA.
 * User: fuqiang
 * Date: 13-5-25
 * Time: 下午3:38
 * To change this template use File | Settings | File Templates.
 */
package gobeaner

import (
	"net"
	"fmt"
	"errors"
	"bufio"
	"strings"
	"io"
)

type GoBeaner struct {
	conn net.Conn
	reader *bufio.Reader
}

func New(host string, port int) (*GoBeaner, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, ConnError{err}
	}
	return &GoBeaner{conn, bufio.NewReader(conn)}, nil
}

func (this *GoBeaner) Put(jobData []byte, pri, delay, ttr int) (jobId int, err error) {
	e := this.send("put %d %d %d %d\r\n%s\r\n", pri, delay, ttr, len(jobData), jobData)
	if e != nil {
		err = ConnError{e}
		return
	}
	firstFrame, e := this.read1stFrame()
	if e != nil {
		err = ConnError{e}
		return
	}
	_, e = fmt.Sscanf(firstFrame, "INSERTED %d\r\n", &jobId)
	if e != nil {
		err = checkError(strings.TrimSuffix(firstFrame, "\r\n"))
		return
	}
	return
}

func (this *GoBeaner) Delete(jobId int) error {
	e := this.send("delete %d\r\n", jobId)
	if e != nil {
		return ConnError{e}
	}
	firstFrame, e := this.read1stFrame()
	if e != nil {
		return ConnError{e}
	}
	_, e = fmt.Sscanf(firstFrame, "DELETED\r\n")
	if e != nil {
		return checkError(strings.TrimSuffix(firstFrame, "\r\n"))
	}
	return nil
}

func (this *GoBeaner) Use(tube string) error {
	e := this.send("use %s\r\n", tube)
	if e != nil {
		return ConnError{e}
	}
	firstFrame, e := this.read1stFrame()
	if e != nil {
		return ConnError{e}
	}
	var tubeName string
	_, e = fmt.Sscanf(firstFrame, "USING %s\r\n", &tubeName)
	if e != nil {
		return checkError(strings.TrimSuffix(firstFrame, "\r\n"))
	}
	return nil
}

func (this *GoBeaner) Reserve() (jobId int, jobData []byte, err error) {
	e := this.send("reserve\r\n")
	if e != nil {
		err = ConnError{e}
		return
	}
	firstFrame, e := this.read1stFrame()
	if e != nil {
		err = ConnError{e}
		return
	}
	var jobLen int
	_, e = fmt.Sscanf(firstFrame, "RESERVED %d %d\r\n", &jobId, &jobLen)
	if e != nil {
		err = checkError(strings.TrimSuffix(firstFrame, "\r\n"))
		return
	}
	if jobLen > 0 {
		jobData, err = this.read2ndFrame(jobLen)
		if err != nil {
			err = ConnError{err}
			return
		}
	}
	return
}

func (this *GoBeaner) send(format string, args... interface {}) error {
	if this.conn == nil {
		return errors.New("connection is not established")
	}
	_, err := fmt.Fprintf(this.conn, format, args...)
	return err
}

func (this *GoBeaner) read1stFrame() (firstFrame string, err error) {
	return this.reader.ReadString('\n')
}

func (this *GoBeaner) read2ndFrame(length int) (data []byte, err error) {
	data = make([]byte, length)
	totalByteRead := 0
	for totalByteRead < length {
		var read int
		read, err = this.reader.Read(data[totalByteRead:])
		totalByteRead += read
		if err == io.EOF && totalByteRead < length {
			err = errors.New("data is incomplete")
		}else if err != nil && err != io.EOF {
			return
		}
	}
	//read ending \r\n
	end, e := this.reader.ReadString('\n')
	if e != nil || end != "\r\n" {
		err = errors.New(fmt.Sprintf("error reading 2nd frame end: %s %s", e, end))
	}
	return
}



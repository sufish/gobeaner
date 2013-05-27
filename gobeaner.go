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

func (this *GoBeaner) Close(){
	this.conn.Close()
}

func (this *GoBeaner) executeCommand(command beanstaldCommand) error {
	var e error
	secondFrame := command.ReqSecondFrame()
	if secondFrame != nil {
		e = this.send("%s%s\r\n", command.ReqFirstFrame(), secondFrame)
	} else {
		e = this.send("%s", command.ReqFirstFrame())
	}
	if e != nil {
		return ConnError{e}
	}
	firstFrame, e := this.read1stFrame()
	if e != nil {
		return ConnError{e}
	}
	secondFrameLen, e := command.ParseRespFirstFrame(firstFrame)
	if e != nil {
		return checkError(strings.TrimSuffix(firstFrame, "\r\n"))
	}
	if secondFrameLen > 0 {
		secondFrame, err := this.read2ndFrame(secondFrameLen)
		if err != nil {
			return ConnError{err}
		}
		command.ParseRespSecondFrame(secondFrame)
	}
	return nil
}

func (this *GoBeaner) Put(jobData []byte, pri, delay, ttr int) (jobId uint64, err error) {
	command := newPutCommand(pri, delay, ttr, jobData)
	err = this.executeCommand(command)
	jobId = command.JobId
	return
}

func (this *GoBeaner) Delete(jobId uint64) error {
	return this.executeCommand(newDeleteCommand(jobId))
}

func (this *GoBeaner) Release(jobId uint64, pri, delay int) error {
	return this.executeCommand(newReleaseCommand(jobId, pri, delay))
}

func (this *GoBeaner) Use(tube string) error {
	return this.executeCommand(newUseCommand(tube))
}

func (this *GoBeaner) ReserveWithTimeOut(timeout int) (jobId uint64, jobData []byte, err error) {
	command := newReserveCommand(timeout)
	err = this.executeCommand(command)
	jobId = command.jobId
	jobData = command.RespSecondFrame
	return
}

func (this *GoBeaner) Reserve() (jobId uint64, jobData []byte, err error) {
	return this.ReserveWithTimeOut(0)
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



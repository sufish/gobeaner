package gobeaner

import (
	"fmt"
)

type beanstaldCommand interface {
	ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error)
	ParseRespSecondFrame(secondFrame []byte)
	ReqFirstFrame() string
	ReqSecondFrame() []byte

}

type baseCommand struct {
	reqFirstFrame   string
	reqSecondFrame  []byte
	RespSecondFrame []byte
}

func (this *baseCommand) ReqFirstFrame() string {
	return this.reqFirstFrame
}

func (this *baseCommand) ReqSecondFrame() []byte {
	return this.reqSecondFrame
}

func (this *baseCommand) ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error) {
	return 0, nil
}

func (this *baseCommand) ParseRespSecondFrame(secondFrame []byte) {
	this.RespSecondFrame = secondFrame
}

type putCommand struct {
	baseCommand
	JobId uint64
}

type useCommand struct {
	baseCommand
}
type deleteCommand struct {
	baseCommand
}
type reserveCommand struct {
	baseCommand
	jobId uint64
}
type releaseCommand struct {
	baseCommand
}

func newPutCommand(pri, delay, ttr int, jobData []byte) *putCommand {
	return &putCommand{baseCommand{fmt.Sprintf("put %d %d %d %d\r\n", pri, delay, ttr, len(jobData)), jobData, nil}, 0}
}

func (this *putCommand) ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error) {
	_, err = fmt.Sscanf(firstFrame, "INSERTED %d\r\n", &this.JobId)
	return
}

func newUseCommand(tube string) *useCommand {
	return &useCommand{baseCommand{fmt.Sprintf("use %s\r\n", tube), nil, nil}}
}

func (this *useCommand) ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error) {
	var tube string
	_, err = fmt.Sscanf(firstFrame, "USING %s\r\n", &tube)
	return
}

func newReserveCommand(timeout int) *reserveCommand {
	var frame string
	if timeout > 0 {
		frame = fmt.Sprintf("reserve-with-timeout %d\r\n", timeout)
	}else {
		frame = "reserve\r\n"
	}
	return &reserveCommand{baseCommand{frame, nil, nil}, 0}
}

func (this *reserveCommand) ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error) {
	_, err = fmt.Sscanf(firstFrame, "RESERVED %d %d\r\n", &this.jobId, &secondFrameLen)
	return
}

func newDeleteCommand(jobId uint64) *deleteCommand {
	return &deleteCommand{baseCommand{fmt.Sprintf("delete %d\r\n", jobId), nil, nil}}
}

func (this *deleteCommand) ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error) {
	_, err = fmt.Sscanf(firstFrame, "DELETED\r\n")
	return
}

func newReleaseCommand(jobId uint64, pri, delay int) *releaseCommand {
	return &releaseCommand{baseCommand{fmt.Sprintf("release %d %d %d\r\n", jobId, pri, delay), nil, nil}}
}

func (this *releaseCommand) ParseRespFirstFrame(firstFrame string) (secondFrameLen int, err error) {
	_, err = fmt.Sscanf(firstFrame, "RELEASED\r\n")
	return
}



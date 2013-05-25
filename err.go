package gobeaner

import (
	"errors"
	"fmt"
)

type ConnError struct {
	Err error
}

type BeanstalkdError struct {
	Err error
}

type UnknownRespError string

func (this ConnError) Error() string {
	return this.Err.Error()
}

func (this BeanstalkdError) Error() string {
	return this.Err.Error()
}

func (this UnknownRespError) Error() string {
	return string(this)
}

var (
	ErrNotFound = BeanstalkdError{errors.New("NOT FOUND")}
)

var errorMap = map[string]BeanstalkdError{
	"NOT_FOUND":ErrNotFound,
}

func checkError(resp string) error {
	if err, found := errorMap[resp]; found{
		return err
	}
	return UnknownRespError(fmt.Sprintf("Unknown response : %s", resp))
}


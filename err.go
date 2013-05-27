package gobeaner

import (
	"errors"
	"fmt"
)

type ConnError struct {
	error
}

type BeanstalkdError struct {
	error
}

type UnknownRespError string

func (this UnknownRespError) Error() string {
	return string(this)
}

var (
	ErrBadFormat  = BeanstalkdError{errors.New("bad command format")}
	ErrBuried     = BeanstalkdError{errors.New("buried")}
	ErrDeadline   = BeanstalkdError{errors.New("deadline soon")}
	ErrDraining   = BeanstalkdError{errors.New("draining")}
	ErrInternal   = BeanstalkdError{errors.New("internal error")}
	ErrJobTooBig  = BeanstalkdError{errors.New("job too big")}
	ErrNoCRLF     = BeanstalkdError{errors.New("expected CR LF")}
	ErrNotFound   = BeanstalkdError{errors.New("not found")}
	ErrNotIgnored = BeanstalkdError{errors.New("not ignored")}
	ErrOOM        = BeanstalkdError{errors.New("server is out of memory")}
	ErrTimeout    = BeanstalkdError{errors.New("timeout")}
	ErrUnknown    = BeanstalkdError{errors.New("unknown command")}
)

var errorMap = map[string]BeanstalkdError{
	"BAD_FORMAT":      ErrBadFormat,
	"BURIED":          ErrBuried,
	"DEADLINE_SOON":   ErrDeadline,
	"DRAINING":        ErrDraining,
	"EXPECTED_CRLF":   ErrNoCRLF,
	"INTERNAL_ERROR":  ErrInternal,
	"JOB_TOO_BIG":     ErrJobTooBig,
	"NOT_FOUND":       ErrNotFound,
	"NOT_IGNORED":     ErrNotIgnored,
	"OUT_OF_MEMORY":   ErrOOM,
	"TIMED_OUT":       ErrTimeout,
	"UNKNOWN_COMMAND": ErrUnknown,
}

func checkError(resp string) error {
	if err, found := errorMap[resp]; found {
		return err
	}
	return UnknownRespError(fmt.Sprintf("Unknown response : %s", resp))
}


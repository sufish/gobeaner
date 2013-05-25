/**
 * Created with IntelliJ IDEA.
 * User: fuqiang
 * Date: 13-5-25
 * Time: 下午4:17
 * To change this template use File | Settings | File Templates.
 */
package gobeaner

import (
	"testing"
)

func TestPut(t *testing.T) {
	beaner, err := New("127.0.0.1", 11300)
	if err != nil {
		t.Fatalf("connection is not established")
	}
	jobId, err := beaner.Put([]byte("test"), 0, 0, 120)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	if jobId == 0 {
		t.Fatalf("job id should not be 0")
	}
}

func TestDeleted(t *testing.T) {
	beaner, err := New("127.0.0.1", 11300)
	if err != nil {
		t.Fatalf("connection is not established")
	}
	jobId, err := beaner.Put([]byte("test"), 0, 0, 120)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	if jobId == 0 {
		t.Fatalf("job id should not be 0")
	}

	err = beaner.Delete(jobId)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
}



package gobeaner

import (
	"testing"
)

func TestCleanUp(t *testing.T) {
	beaner := setUpConn(t)
	clearUp(beaner)
}

func TestPut(t *testing.T) {
	beaner := setUpConn(t)
	defer clearUp(beaner)
	jobId, err := beaner.Put([]byte("test"), 0, 0, 120)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	if jobId == 0 {
		t.Fatalf("job id should not be 0")
	}
}

func TestDelete(t *testing.T) {
	beaner := setUpConn(t)
	defer clearUp(beaner)
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

func TestReserve(t *testing.T) {
	beaner := setUpConn(t)
	defer clearUp(beaner)
	jobId, err := beaner.Put([]byte("test"), 0, 0, 120)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	if jobId == 0 {
		t.Fatalf("job id should not be 0")
	}
	_, jobData, err := beaner.Reserve()
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	if string(jobData) != "test" {
		t.Fatalf("wrong test data")
	}
}

func TestRelease(t *testing.T) {
	beaner := setUpConn(t)
	defer clearUp(beaner)
	jobId, err := beaner.Put([]byte("test"), 0, 0, 120)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	if jobId == 0 {
		t.Fatalf("job id should not be 0")
	}
	jobId, _, err = beaner.Reserve()
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	err = beaner.Release(jobId, 0, 0)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
}

func clearUp(beaner *GoBeaner) error {
	for {
		jobId, _, err := beaner.ReserveWithTimeOut(1)
		if err == nil {
			err = beaner.Delete(jobId)
			if err != nil {
				return err
			}
		}else if err == ErrTimeout {
			return nil
		}else {
			return err
		}
	}
	return nil
}

func setUpConn(t *testing.T) (*GoBeaner) {
	beaner, err := New("127.0.0.1", 11300)
	if err != nil {
		t.Fatalf("connection is not established")
	}
	err = beaner.Use("default")
	if err != nil {
		t.Fatalf("use command failed:" + err.Error())
	}
	return beaner
}



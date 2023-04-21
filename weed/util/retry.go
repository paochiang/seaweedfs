package util

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var RetryWaitTime = 6 * time.Second

func Retry(name string, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	var lastErr error

	for waitTime < RetryWaitTime {
		err = job()
		if err == nil {
			if hasErr {
				glog.V(0).Infof("retry %s successfully, last err:%v", name, lastErr)
			}
			break
		}
		if strings.Contains(err.Error(), "transport") {
			hasErr = true
			glog.V(0).Infof("retry %s: err: %v", name, err)
			time.Sleep(waitTime)
			waitTime += waitTime / 2
		} else {
			break
		}
		lastErr = err
	}

	if waitTime >= RetryWaitTime {
		glog.V(0).Infof("Retry timout, Err:%s", err.Error())
	}

	return err
}

func UploadRetry(name string, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	var lastErr error

	for waitTime < RetryWaitTime {
		err = job()
		if err == nil {
			if hasErr {
				glog.V(0).Infof("retry %s successfully, last err:%v", name, lastErr)
			}
			break
		}
		if strings.Contains(err.Error(), "transport") || strings.Contains(err.Error(), "is read only") || strings.Contains(err.Error(), "Not current leader") {
			hasErr = true
			glog.V(0).Infof("retry %s: err: %v", name, err)
			time.Sleep(waitTime)
			waitTime += waitTime / 4
		} else {
			break
		}
		lastErr = err
	}

	if waitTime >= RetryWaitTime {
		glog.V(0).Infof("Retry timout, Err:%s", err.Error())
	}

	return err
}

func RetryForever(name string, job func() error, onErrFn func(err error) (shouldContinue bool)) {
	waitTime := time.Second
	for {
		err := job()
		if err == nil {
			waitTime = time.Second
			break
		}
		if onErrFn(err) {
			if strings.Contains(err.Error(), "transport") {
				glog.V(0).Infof("retry %s: err: %v", name, err)
			}
			time.Sleep(waitTime)
			if waitTime < RetryWaitTime {
				waitTime += waitTime / 2
			}
			continue
		}
	}
}

// return the first non empty string
func Nvl(values ...string) string {
	for _, s := range values {
		if s != "" {
			return s
		}
	}
	return ""
}

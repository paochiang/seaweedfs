package mount

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) loopCheckQuota() {

	for {

		time.Sleep(61 * time.Second)

		if wfs.option.Quota <= 0 {
			continue
		}

		err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.StatisticsRequest{
				Collection:  wfs.option.Collection,
				Replication: wfs.option.Replication,
				Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
				DiskType:    string(wfs.option.DiskType),
			}

			resp, err := client.Statistics(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("reading quota usage %v: %v", request, err)
				return err
			}
			glog.V(4).Infof("read quota usage: %+v", resp)

			isOverQuota := int64(resp.UsedSize) > wfs.option.Quota
			if isOverQuota && !wfs.IsOverQuota {
				glog.Warningf("Quota Exceeded! quota:%d used:%d", wfs.option.Quota, resp.UsedSize)
			} else if !isOverQuota && wfs.IsOverQuota {
				glog.Warningf("Within quota limit! quota:%d used:%d", wfs.option.Quota, resp.UsedSize)
			}
			wfs.IsOverQuota = isOverQuota

			return nil
		})

		if err != nil {
			glog.Warningf("read quota usage: %v", err)
		}

	}

}

func (wfs *WFS) metaCacheCheckRefresh() {
	watchFile := filepath.Join(wfs.option.uniqueCacheDir, "refresh")
	if !util.FileExists(watchFile) {
		file, err := os.OpenFile(watchFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			glog.Warningf("cannot create watcher file %s: %v", watchFile, err)
			return
		}
		file.Close()
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		glog.Warningf("new watcher failed: %v", err)
		return
	}
	defer watcher.Close()

	err = watcher.Add(watchFile)
	if err != nil {
		glog.Warningf("new watcher add file failed: %v", err)
		return
	}
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				glog.V(0).Infof("event not ok")
			} else {
				glog.V(0).Infof("%s write: refresh metacache", event.Name)
			}
			wfs.metaCache.InvalidateAllDirectory()

		case err, ok := <-watcher.Errors:
			if !ok {
				glog.V(0).Infof("write not ok")
			}
			glog.V(0).Infof("error: %s", err)
		}
	}
}

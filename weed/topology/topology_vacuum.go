package topology

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func (t *Topology) batchVacuumVolumeCheck(grpcDialOption grpc.DialOption, vid needle.VolumeId,
	locationlist *VolumeLocationList, garbageThreshold float64) (*VolumeLocationList, float64, bool) {
	ch := make(chan int, locationlist.Length())
	gch := make(chan float64, locationlist.Length())
	errCount := int32(0)
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			err := operation.WithVolumeServerClient(false, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, err := volumeServerClient.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
					VolumeId: uint32(vid),
				})
				if err != nil {
					atomic.AddInt32(&errCount, 1)
					ch <- -1
					gch <- -1
					return err
				}
				if resp.GarbageRatio >= garbageThreshold {
					ch <- index
					gch <- resp.GarbageRatio
				} else {
					ch <- -1
					gch <- -1
				}
				return nil
			})
			if err != nil {
				glog.V(0).Infof("Checking vacuuming %d on %s: %v", vid, url, err)
			}
		}(index, dn.ServerAddress(), vid)
	}
	vacuumLocationList := NewVolumeLocationList()

	waitTimeout := time.NewTimer(time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case index := <-ch:
			if index != -1 {
				vacuumLocationList.list = append(vacuumLocationList.list, locationlist.list[index])
			}
		case <-waitTimeout.C:
			return vacuumLocationList, 0, false
		}
	}

	var garbageRatio float64 = 0
	for i := 0; i < locationlist.Length(); i++ {
		select {
		case gRation := <-gch:
			garbageRatio = math.Max(gRation, garbageRatio)
		case <-waitTimeout.C:
			return vacuumLocationList, 0, false
		}
	}

	return vacuumLocationList, garbageRatio, errCount == 0 && len(vacuumLocationList.list) > 0
}

func (t *Topology) batchVacuumVolumeCompact(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId,
	locationlist *VolumeLocationList, preallocate int64) bool {
	vl.accessLock.Lock()
	vl.removeFromWritable(vid)
	vl.accessLock.Unlock()

	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			err := operation.WithVolumeServerClient(true, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				stream, err := volumeServerClient.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
					VolumeId:    uint32(vid),
					Preallocate: preallocate,
				})
				if err != nil {
					return err
				}

				for {
					resp, recvErr := stream.Recv()
					if recvErr != nil {
						if recvErr == io.EOF {
							break
						} else {
							return recvErr
						}
					}
					glog.V(0).Infof("%d vacuum %d on %s processed %d bytes, loadAvg %.02f%%",
						index, vid, url, resp.ProcessedBytes, resp.LoadAvg_1M*100)
				}
				return nil
			})
			if err != nil {
				glog.Errorf("Error when vacuuming %d on %s: %v", vid, url, err)
				ch <- false
			} else {
				glog.V(0).Infof("Complete vacuuming %d on %s", vid, url)
				ch <- true
			}
		}(index, dn.ServerAddress(), vid)
	}
	isVacuumSuccess := true

	waitTimeout := time.NewTimer(3 * time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case canCommit := <-ch:
			isVacuumSuccess = isVacuumSuccess && canCommit
		case <-waitTimeout.C:
			return false
		}
	}
	return isVacuumSuccess
}

func (t *Topology) batchVacuumVolumeCommit(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) bool {
	isCommitSuccess := true
	isReadOnly := false
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start Committing vacuum", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			resp, err := volumeServerClient.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: uint32(vid),
			})
			if resp != nil && resp.IsReadOnly {
				isReadOnly = true
			}
			return err
		})
		if err != nil {
			glog.Errorf("Error when committing vacuum %d on %s: %v", vid, dn.Url(), err)
			isCommitSuccess = false
		} else {
			glog.V(0).Infof("Complete Committing vacuum %d on %s", vid, dn.Url())
		}
	}
	if isCommitSuccess {
		for _, dn := range locationlist.list {
			vl.SetVolumeAvailable(dn, vid, isReadOnly)
		}
	}
	return isCommitSuccess
}

func (t *Topology) batchVacuumVolumeCleanup(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) {
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start cleaning up", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			_, err := volumeServerClient.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumeId: uint32(vid),
			})
			return err
		})
		if err != nil {
			glog.Errorf("Error when cleaning up vacuum %d on %s: %v", vid, dn.Url(), err)
		} else {
			glog.V(0).Infof("Complete cleaning up vacuum %d on %s", vid, dn.Url())
		}
	}
}

func (t *Topology) Vacuum(grpcDialOption grpc.DialOption, garbageThreshold float64, volumeId uint32, collection string, preallocate int64) {

	// if there is vacuum going on, return immediately
	swapped := atomic.CompareAndSwapInt64(&t.vacuumLockCounter, 0, 1)
	if !swapped {
		return
	}
	defer atomic.StoreInt64(&t.vacuumLockCounter, 0)

	// now only one vacuum process going on

	glog.V(1).Infof("Start vacuum on demand with threshold: %f collection: %s volumeId: %d",
		garbageThreshold, collection, volumeId)
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		if collection != "" && collection != c.Name {
			continue
		}
		for _, vl := range c.storageType2VolumeLayout.Items() {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)
				if volumeId > 0 {
					vid := needle.VolumeId(volumeId)
					volumeLayout.accessLock.RLock()
					locationList, ok := volumeLayout.vid2location[vid]
					volumeLayout.accessLock.RUnlock()
					if ok {
						t.vacuumOneVolumeId(grpcDialOption, volumeLayout, c, garbageThreshold, locationList, vid, preallocate)
					}
				} else {
					t.vacuumOneVolumeLayout(grpcDialOption, volumeLayout, c, garbageThreshold, preallocate)
				}
			}
		}
	}
}

func (t *Topology) vacuumOneVolumeLayout(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, preallocate int64) {

	volumeLayout.accessLock.RLock()
	tmpMap := make(map[needle.VolumeId]*VolumeLocationList)
	for vid, locationList := range volumeLayout.vid2location {
		tmpMap[vid] = locationList.Copy()
	}
	volumeLayout.accessLock.RUnlock()

	volumesVacuum := []needle.VolumeId{}
	volumeLocations := make(map[needle.VolumeId]*VolumeLocationList)
	volumeGarbageRatio := make(map[needle.VolumeId]float64)
	for vid, locationList := range tmpMap {
		volumeLayout.accessLock.RLock()
		isReadOnly := volumeLayout.readonlyVolumes.IsTrue(vid)
		isEnoughCopies := volumeLayout.enoughCopies(vid)
		volumeLayout.accessLock.RUnlock()

		if isReadOnly || !isEnoughCopies {
			continue
		}
		if vacuumLocationList, ratio, needVacuum := t.batchVacuumVolumeCheck(grpcDialOption, vid, locationList, garbageThreshold); needVacuum {
			volumesVacuum = append(volumesVacuum, vid)
			volumeLocations[vid] = vacuumLocationList
			volumeGarbageRatio[vid] = ratio
		}
	}

	glog.V(1).Infof("Vacuum: %d volumes should be vacuumed for this loop", len(volumesVacuum))
	count := len(volumesVacuum)

	var wg sync.WaitGroup
	var lock sync.Mutex
	activeVolumes := make(map[needle.VolumeId]*VolumeLocationList)

	for {
		mapLength := len(volumeLocations)
		if mapLength < 1 {
			break
		}

		if t.isDisableVacuum {
			glog.V(0).Infof("Vacuum interrupted by disableVacuum")
			break
		}

		vid, err := getDiffVolume(volumeLocations, activeVolumes)
		if err == nil {
			lock.Lock()
			activeVolumes[vid] = volumeLocations[vid].Copy()
			lock.Unlock()
			delete(volumeLocations, vid)
			wg.Add(1)

			go func(volumeId needle.VolumeId, list *VolumeLocationList) {
				glog.V(1).Infof("Start vacuum volume: %d with garbage ratio:  %6.3f , garbageThreshold: %f  and  %d volumes left to be vacuumed", volumeId, volumeGarbageRatio[volumeId], garbageThreshold, count)
				defer func() {
					lock.Lock()
					delete(activeVolumes, volumeId)
					lock.Unlock()
					wg.Done()
				}()
				lock.Lock()
				count = count - 1
				lock.Unlock()
				t.vacuumOneVolumeId(grpcDialOption, volumeLayout, c, garbageThreshold, list, volumeId, preallocate)
			}(vid, activeVolumes[vid])

		} else {
			oldLen := len(activeVolumes)
			if oldLen >= 1 {
				checkTime := time.Now()
				for {
					if checkTime.Add(2 * time.Minute).Before(time.Now()) {
						//timeout
						break
					}
					if len(activeVolumes) < oldLen {
						break
					} else {
						time.Sleep(3 * time.Second)
					}
				}

			}
		}
	}

	wg.Wait()

}

func getDiffVolume(locations, active map[needle.VolumeId]*VolumeLocationList) (needle.VolumeId, error) {

	//datanodes vacuuming
	nodeMap := make(map[NodeId]bool)
	for _, list := range active {
		for _, node := range list.list {
			nodeMap[node.id] = true
		}
	}

	for vid, volumeLocations := range locations {

		overlap := false
		for _, node := range volumeLocations.list {
			if _, ok := nodeMap[node.id]; ok {
				overlap = true
				break
			}
		}

		if !overlap {
			return vid, nil
		}
	}

	return needle.VolumeId(0), fmt.Errorf("hasn't noverlaped volume")
}

func (t *Topology) vacuumOneVolumeId(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, locationList *VolumeLocationList, vid needle.VolumeId, preallocate int64) {
	volumeLayout.accessLock.RLock()
	isReadOnly := volumeLayout.readonlyVolumes.IsTrue(vid)
	isEnoughCopies := volumeLayout.enoughCopies(vid)
	volumeLayout.accessLock.RUnlock()

	if isReadOnly || !isEnoughCopies {
		return
	}

	glog.V(1).Infof("check vacuum on collection:%s volume:%d", c.Name, vid)
	if vacuumLocationList, _, needVacuum := t.batchVacuumVolumeCheck(
		grpcDialOption, vid, locationList, garbageThreshold); needVacuum {
		if t.batchVacuumVolumeCompact(grpcDialOption, volumeLayout, vid, vacuumLocationList, preallocate) {
			t.batchVacuumVolumeCommit(grpcDialOption, volumeLayout, vid, vacuumLocationList)
		} else {
			t.batchVacuumVolumeCleanup(grpcDialOption, volumeLayout, vid, vacuumLocationList)
		}
	}
}

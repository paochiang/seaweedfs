package topology

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func ReplicatedWrite(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, s *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request, contentMd5 string) (isUnchanged bool, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	// check whether this is a replicated write request
	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		// this is the initial request
		remoteLocations, err = getWritableRemoteReplications(s, grpcDialOption, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	// read fsync value
	fsync := false
	if r.FormValue("fsync") == "true" {
		fsync = true
	}

	if s.GetVolume(volumeId) != nil {
		isUnchanged, err = s.WriteVolumeNeedle(volumeId, n, true, fsync)
		if err != nil {
			err = fmt.Errorf("failed to write to local disk: %v", err)
			glog.V(0).Infoln(err)
			return
		}
	}

	if len(remoteLocations) > 0 { //send to other replica locations
		if err = DistributedOperation(remoteLocations, func(location operation.Location) error {
			u := url.URL{
				Scheme: "http",
				Host:   location.Url,
				Path:   r.URL.Path,
			}
			q := url.Values{
				"type": {"replicate"},
				"ttl":  {n.Ttl.String()},
			}
			if n.LastModified > 0 {
				q.Set("ts", strconv.FormatUint(n.LastModified, 10))
			}
			if n.IsChunkedManifest() {
				q.Set("cm", "true")
			}
			u.RawQuery = q.Encode()

			pairMap := make(map[string]string)
			if n.HasPairs() {
				tmpMap := make(map[string]string)
				err := json.Unmarshal(n.Pairs, &tmpMap)
				if err != nil {
					glog.V(0).Infoln("Unmarshal pairs error:", err)
				}
				for k, v := range tmpMap {
					pairMap[needle.PairNamePrefix+k] = v
				}
			}

			pairMap[needle.PairNamePrefix+"Crc"] = fmt.Sprintf("%x", needle.NewCRC(n.Data))
			pairMap[needle.PairNamePrefix+"Md5"] = contentMd5

			// volume server do not know about encryption
			// TODO optimize here to compress data only once
			uploadOption := &operation.UploadOption{
				UploadUrl:         u.String(),
				Filename:          string(n.Name),
				Cipher:            false,
				IsInputCompressed: n.IsCompressed(),
				MimeType:          string(n.Mime),
				PairMap:           pairMap,
				Jwt:               jwt,
			}
			//发送其他volume前，检查下n.Data内容是否改变
			preCheckMd5 := ""
			hm := md5.New()
			if n.IsCompressed() {
				if unzipped, e := util.DecompressData(n.Data); e == nil {
					hm.Write(unzipped)
					preCheckMd5 = base64.StdEncoding.EncodeToString(hm.Sum(nil))
					if preCheckMd5 != contentMd5 {
						glog.Errorf("inconsistent-precheck md5, preCheckMd5:%s, src:%s, url:%s", preCheckMd5, contentMd5, u.String())
					}
				} else {
					glog.Errorf("DecompressData-error, src:%s, url:%s, err;%v", contentMd5, u.String(), e)
				}
			} else {
				hm.Write(n.Data)
				preCheckMd5 = base64.StdEncoding.EncodeToString(hm.Sum(nil))
				if preCheckMd5 != contentMd5 {
					glog.Errorf("inconsistent-precheck md5, preCheckMd5:%s, src:%s, url:%s", preCheckMd5, contentMd5, u.String())
				}
			}

			uploadResult, err := operation.UploadData(n.Data, uploadOption)
			if err != nil {
				glog.Errorf("operation-UploadData, err:%v, url:%s", err, u.String())
			} else if uploadResult.ContentMd5 != "" && contentMd5 != uploadResult.ContentMd5 {
				glog.Errorf("inconsistent-md5, src:%s, dst:%s, preCheckMd5:%s, url:%s", contentMd5,
					uploadResult.ContentMd5, preCheckMd5, u.String(), r.URL)
			}
			return err
		}); err != nil {
			err = fmt.Errorf("failed to write to replicas for volume %d: %v", volumeId, err)
			glog.V(0).Infoln(err)
		}
	}
	return
}

func ReplicatedDelete(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, store *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request) (size types.Size, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		remoteLocations, err = getWritableRemoteReplications(store, grpcDialOption, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	size, err = store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return
	}

	if len(remoteLocations) > 0 { //send to other replica locations
		if err = DistributedOperation(remoteLocations, func(location operation.Location) error {
			return util.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", string(jwt))
		}); err != nil {
			size = 0
		}
	}
	return
}

type DistributedOperationResult map[string]error

func (dr DistributedOperationResult) Error() error {
	var errs []string
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

type RemoteResult struct {
	Host  string
	Error error
}

func DistributedOperation(locations []operation.Location, op func(location operation.Location) error) error {
	length := len(locations)
	results := make(chan RemoteResult)
	for _, location := range locations {
		go func(location operation.Location, results chan RemoteResult) {
			results <- RemoteResult{location.Url, op(location)}
		}(location, results)
	}
	ret := DistributedOperationResult(make(map[string]error))
	for i := 0; i < length; i++ {
		result := <-results
		ret[result.Host] = result.Error
	}

	return ret.Error()
}

func getWritableRemoteReplications(s *storage.Store, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, masterFn operation.GetMasterFn) (remoteLocations []operation.Location, err error) {

	v := s.GetVolume(volumeId)
	if v != nil && v.ReplicaPlacement.GetCopyCount() == 1 {
		return
	}

	// not on local store, or has replications
	lookupResult, lookupErr := operation.LookupVolumeId(masterFn, grpcDialOption, volumeId.String())
	if lookupErr == nil {
		selfUrl := util.JoinHostPort(s.Ip, s.Port)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				remoteLocations = append(remoteLocations, location)
			}
		}
	} else {
		err = fmt.Errorf("failed to lookup for %d: %v", volumeId, lookupErr)
		return
	}

	if v != nil {
		// has one local and has remote replications
		copyCount := v.ReplicaPlacement.GetCopyCount()
		if len(lookupResult.Locations) < copyCount {
			err = fmt.Errorf("replicating opetations [%d] is less than volume %d replication copy count [%d]",
				len(lookupResult.Locations), volumeId, copyCount)
		}
	}

	return
}

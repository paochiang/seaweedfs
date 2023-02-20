package storage

import (
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func loadVolumeWithoutIndex(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapKind) (v *Volume, err error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = super_block.SuperBlock{}
	v.needleMapKind = needleMapKind
	err = v.load(false, false, needleMapKind, 0)
	return
}

type VolumeFileScanner4HotFix struct {
	version needle.Version
	nm      *needle_map.MemDb
}

func (scanner *VolumeFileScanner4HotFix) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil

}
func (scanner *VolumeFileScanner4HotFix) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4HotFix) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	glog.V(2).Infof("key %d offset %d size %d disk_size %d compressed %v", n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsCompressed())
	if n.Size.IsValid() {
		pe := scanner.nm.Set(n.Id, types.ToOffset(offset), n.Size)
		glog.V(2).Infof("saved %d with error %v", n.Size, pe)
	} else {
		glog.V(2).Infof("skipping deleted file ...")
		return scanner.nm.Delete(n.Id)
	}
	return nil
}

func fixOneVolume(basepath string, collection string, vid needle.VolumeId) error {

	indexFileName := path.Join(basepath, strconv.Itoa(int(vid))+".idx")

	nm := needle_map.NewMemDb()
	defer nm.Close()
	scanner := &VolumeFileScanner4HotFix{
		nm: nm,
	}

	if err := ScanVolumeFile(basepath, collection, vid, NeedleMapInMemory, scanner); err != nil {
		return fmt.Errorf("scan .dat File: %v", err)
	}

	if err := nm.SaveToIdx(indexFileName); err != nil {
		os.Remove(indexFileName)
		return fmt.Errorf("save to .idx File: %v", err)
	}

	return nil
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapKind, preallocate int64) (err error) {
	alreadyHasSuperBlock := false

	hasLoadedVolume := false
	defer func() {
		if !hasLoadedVolume {
			if v.nm != nil {
				v.nm.Close()
				v.nm = nil
			}
			if v.DataBackend != nil {
				v.DataBackend.Close()
				v.DataBackend = nil
			}
		}
	}()

	hasVolumeInfoFile := v.maybeLoadVolumeInfo()

	if v.HasRemoteFile() {
		v.noWriteCanDelete = true
		v.noWriteOrDelete = false
		glog.V(0).Infof("loading volume %d from remote %v", v.Id, v.volumeInfo)
		v.LoadRemoteFile()
		alreadyHasSuperBlock = true
	} else if exists, canRead, canWrite, modifiedTime, fileSize := util.CheckFile(v.FileName(".dat")); exists {
		// open dat file
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s", v.FileName(".dat"))
		}
		var dataFile *os.File
		if canWrite {
			dataFile, err = os.OpenFile(v.FileName(".dat"), os.O_RDWR|os.O_CREATE, 0644)
		} else {
			glog.V(0).Infof("opening %s in READONLY mode", v.FileName(".dat"))
			dataFile, err = os.Open(v.FileName(".dat"))
			v.noWriteOrDelete = true
		}
		v.lastModifiedTsSeconds = uint64(modifiedTime.Unix())
		if fileSize >= super_block.SuperBlockSize {
			alreadyHasSuperBlock = true
		}
		v.DataBackend = backend.NewDiskFile(dataFile)
	} else {
		if createDatIfMissing {
			v.DataBackend, err = backend.CreateVolumeFile(v.FileName(".dat"), preallocate, v.MemoryMapMaxSizeMb)
		} else {
			return fmt.Errorf("volume data file %s does not exist", v.FileName(".dat"))
		}
	}

	if err != nil {
		if !os.IsPermission(err) {
			return fmt.Errorf("cannot load volume data %s: %v", v.FileName(".dat"), err)
		} else {
			return fmt.Errorf("load data file %s: %v", v.FileName(".dat"), err)
		}
	}

	if alreadyHasSuperBlock {
		err = v.readSuperBlock()
		if err == nil {
			v.volumeInfo.Version = uint32(v.SuperBlock.Version)
		}
		glog.V(0).Infof("readSuperBlock volume %d version %v", v.Id, v.SuperBlock.Version)
		if v.HasRemoteFile() {
			// maybe temporary network problem
			glog.Errorf("readSuperBlock remote volume %d: %v", v.Id, err)
			err = nil
		}
	} else {
		if !v.SuperBlock.Initialized() {
			return fmt.Errorf("volume %s not initialized", v.FileName(".dat"))
		}
		err = v.maybeWriteSuperBlock()
	}
	if err == nil && alsoLoadIndex {
		// adjust for existing volumes with .idx together with .dat files
		if v.dirIdx != v.dir {
			if util.FileExists(v.DataFileName() + ".idx") {
				v.dirIdx = v.dir
			}
		}
		// check volume idx files
		if err := v.checkIdxFile(); err != nil {
			glog.Warningf("check volume idx file %s: %v, try to fix it", v.FileName(".idx"), err)
			if errFix := fixOneVolume(v.dir, v.Collection, v.Id); errFix != nil {
				return fmt.Errorf("fix volume idx file %s: %v", v.FileName(".idx"), err)
			}
		}
		if err := v.checkIdxFile(); err != nil {
			return fmt.Errorf("check volume idx file again %s: %v", v.FileName(".idx"), err)
		}
		var indexFile *os.File
		if v.noWriteOrDelete {
			glog.V(0).Infoln("open to read file", v.FileName(".idx"))
			if indexFile, err = os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644); err != nil {
				return fmt.Errorf("cannot read Volume Index %s: %v", v.FileName(".idx"), err)
			}
		} else {
			glog.V(1).Infoln("open to write file", v.FileName(".idx"))
			if indexFile, err = os.OpenFile(v.FileName(".idx"), os.O_RDWR|os.O_CREATE, 0644); err != nil {
				return fmt.Errorf("cannot write Volume Index %s: %v", v.FileName(".idx"), err)
			}
		}

		if v.lastAppendAtNs, err = CheckAndFixVolumeDataIntegrity(v, indexFile); err != nil {
			glog.Warningf("volumeDataIntegrityChecking failed %v, try to fix it", err)
			if errFix := fixOneVolume(v.dir, v.Collection, v.Id); errFix != nil {
				return fmt.Errorf("fix volume idx file %s: %v", v.FileName(".idx"), err)
			}
		}

		if v.lastAppendAtNs, err = CheckAndFixVolumeDataIntegrity(v, indexFile); err != nil {
			v.noWriteOrDelete = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed again %v", err)
		}

		if v.noWriteOrDelete || v.noWriteCanDelete {
			if v.nm, err = NewSortedFileNeedleMap(v.IndexFileName(), indexFile); err != nil {
				glog.V(0).Infof("loading sorted db %s error: %v", v.FileName(".sdx"), err)
			}
		} else {
			switch needleMapKind {
			case NeedleMapInMemory:
				glog.V(0).Infoln("loading index", v.FileName(".idx"), "to memory")
				if v.nm, err = LoadCompactNeedleMap(indexFile); err != nil {
					glog.V(0).Infof("loading index %s to memory error: %v", v.FileName(".idx"), err)
				}
			case NeedleMapLevelDb:
				glog.V(0).Infoln("loading leveldb", v.FileName(".ldb"))
				opts := &opt.Options{
					BlockCacheCapacity:            2 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   1 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.nm, err = NewLevelDbNeedleMap(v.FileName(".ldb"), indexFile, opts); err != nil {
					glog.V(0).Infof("loading leveldb %s error: %v", v.FileName(".ldb"), err)
				}
			case NeedleMapLevelDbMedium:
				glog.V(0).Infoln("loading leveldb medium", v.FileName(".ldb"))
				opts := &opt.Options{
					BlockCacheCapacity:            4 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   2 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.nm, err = NewLevelDbNeedleMap(v.FileName(".ldb"), indexFile, opts); err != nil {
					glog.V(0).Infof("loading leveldb %s error: %v", v.FileName(".ldb"), err)
				}
			case NeedleMapLevelDbLarge:
				glog.V(0).Infoln("loading leveldb large", v.FileName(".ldb"))
				opts := &opt.Options{
					BlockCacheCapacity:            8 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   4 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.nm, err = NewLevelDbNeedleMap(v.FileName(".ldb"), indexFile, opts); err != nil {
					glog.V(0).Infof("loading leveldb %s error: %v", v.FileName(".ldb"), err)
				}
			}
		}
	}

	if !hasVolumeInfoFile {
		v.volumeInfo.Version = uint32(v.SuperBlock.Version)
		v.SaveVolumeInfo()
	}

	stats.VolumeServerVolumeCounter.WithLabelValues(v.Collection, "volume").Inc()

	if err == nil {
		hasLoadedVolume = true
	}

	return err
}

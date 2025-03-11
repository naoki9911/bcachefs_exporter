package main

import (
	"flag"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/naoki9911/bcachefs_exporter/pkg/bcachefs"
	"github.com/naoki9911/bcachefs_exporter/pkg/bcachefs/sysfs"
	"github.com/naoki9911/bcachefs_exporter/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	targetPath = flag.String("target-path", "", "target path to export")
)

func main() {
	flag.Parse()

	if *targetPath == "" {
		log.Fatalf("--target-path is not specified")
	}
	log.Infof("bcachefs_exporter (version %s) started", version.Version)
	bchBin, err := exec.LookPath("bcachefs")
	if err != nil {
		log.Fatalf("failed to find command 'bcachefs': %v", err)
	}
	log.Infof("Target path: %s", *targetPath)

	ticker := time.NewTicker(10 * time.Second)
	run(bchBin, *targetPath)
	go func() {
		for {
			<-ticker.C
			run(bchBin, *targetPath)
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":9091", nil)
}

var (
	promBchSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_fs_usage_size",
	},
		[]string{
			"mountpoint",
			"uuid",
			"type",
		},
	)
	promBchReplicasUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_fs_usage_replicas_usage",
	},
		[]string{
			"mountpoint",
			"uuid",
			"dataType",
			"requiredTotal",
			"durability",
			"devices",
		},
	)
	promBchCompression = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_fs_usage_compression",
	},
		[]string{
			"mountpoint",
			"uuid",
			"compressionType",
			"dataType",
		},
	)
	promBchBtree = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_fs_usage_btree",
	},
		[]string{
			"mountpoint",
			"uuid",
			"dataType",
		},
	)
	promBchRebalance = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_fs_usage_rebalance",
	},
		[]string{
			"mountpoint",
			"uuid",
			"dataType",
		},
	)
	promBchDevice = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_fs_usage_device",
	},
		[]string{
			"mountpoint",
			"uuid",
			"label",
			"device",
			"type",
			"dataType",
		},
	)
	promBchSysFsBtreeWriteStat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_btree_write_stats",
	},
		[]string{
			"mountpoint",
			"uuid",
			"type",
			"dataType",
		},
	)
	promBchSysFsBtreeCacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_btree_cache_size",
	},
		[]string{
			"mountpoint",
			"uuid",
		},
	)
	promBchSysFsCompressionStat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_compression_stats",
	},
		[]string{
			"mountpoint",
			"uuid",
			"compressionType",
			"dataType",
		},
	)
	promBchSysFsRebalanceStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_rebalance_status",
	},
		[]string{
			"mountpoint",
			"uuid",
			"state",
			"dataType",
			"item",
		},
	)
	promBchSysFsTimeStat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_time_stat",
	},
		[]string{
			"mountpoint",
			"uuid",
			"item",
			"dataType",
		},
	)
	promBchSysFsDevStat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_dev_stat",
	},
		[]string{
			"mountpoint",
			"uuid",
			"devName",
			"devUuid",
			"devLabel",
			"item",
		},
	)
	promBchSysFsDevIoDone = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_dev_io_done",
	},
		[]string{
			"mountpoint",
			"uuid",
			"devName",
			"devUuid",
			"devLabel",
			"direction",
			"item",
		},
	)
	promBchSysFsDevIoErrors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_dev_io_erros",
	},
		[]string{
			"mountpoint",
			"uuid",
			"devName",
			"devUuid",
			"devLabel",
			"item",
		},
	)
	promBchSysFsDevIoLatency = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_dev_io_latency",
	},
		[]string{
			"mountpoint",
			"uuid",
			"devName",
			"devUuid",
			"devLabel",
			"direction",
			"dataType",
		},
	)
	promBchSysFsCounter = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bcachefs_sysfs_counter",
	},
		[]string{
			"mountpoint",
			"uuid",
			"item",
			"dataType",
		},
	)
)

func run(bchBinPath, path string) {
	results, err := exec.Command(bchBinPath, "fs", "usage", path).Output()
	if err != nil {
		log.Fatalf("Failed to get usage: %v", err)
	}

	outputDir := filepath.Join("/tmp", "bcachefs_exporter")
	err = os.MkdirAll(outputDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", outputDir, err)
	}
	outputFilePath := filepath.Join(outputDir, "output.log")
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", outputFilePath, err)
	}
	defer outputFile.Close()
	_, err = outputFile.Write(results)
	if err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	fsUsage := bcachefs.ParseFsUsage(path, string(results))
	sysFs, err := sysfs.ParseSysFs(fsUsage.FileSystem)
	if err != nil {
		log.Fatalf("Failed to parse sysfs: %v", err)
	}
	sysFsTimestats, err := sysfs.ParseSysFsTimeStats(fsUsage.FileSystem)
	if err != nil {
		log.Fatalf("Failed to parse sysfs time_stats: %v", err)
	}

	sysFsDevs, err := sysfs.ParseSysFsDevs(fsUsage.FileSystem)
	if err != nil {
		log.Fatalf("Failed to parse sysfs devs: %v", err)
	}

	sysFsCounters, err := sysfs.ParseSysFsCounters(fsUsage.FileSystem)
	if err != nil {
		log.Fatalf("Failed to parse sysfs counters: %v", err)
	}

	promBchSize.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, "capacity").Set(float64(fsUsage.Capacity))
	promBchSize.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, "used").Set(float64(fsUsage.Used))
	promBchSize.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, "online reserved").Set(float64(fsUsage.OnlineReserved))

	for _, r := range fsUsage.Replicas {
		promBchReplicasUsage.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, r.DataType, r.RequiredTotal, r.Durability, r.Devices).Set(float64(r.Size))
	}

	for _, c := range fsUsage.Compressions {
		promBchCompression.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, c.CompressionType, "compressed").Set(float64(c.Comporessed))
		promBchCompression.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, c.CompressionType, "uncompressed").Set(float64(c.Uncompressed))
		promBchCompression.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, c.CompressionType, "average extent size").Set(float64(c.AverageExtentSize))
	}
	for _, b := range fsUsage.Btrees {
		promBchBtree.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, b.DataType).Set(float64(b.Size))
	}

	promBchRebalance.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, "pending").Set(float64(fsUsage.Rebalance.PendingSize))

	for _, dev := range fsUsage.Devices {
		for _, ddev := range dev.Datas {
			promBchDevice.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, dev.Label, dev.Device, ddev.DataType, "data").Set(float64(ddev.Size))
			promBchDevice.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, dev.Label, dev.Device, ddev.DataType, "buckets").Set(float64(ddev.Buckets))
			if ddev.HasFragmented {
				promBchDevice.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, dev.Label, dev.Device, ddev.DataType, "fragmented").Set(float64(ddev.Fragmented))
			}
		}
	}

	if sysFs.BtreeWriteStat != nil {
		for _, ws := range sysFs.BtreeWriteStat {
			promBchSysFsBtreeWriteStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, ws.Stat, "nr").Set(float64(ws.NR))
			promBchSysFsBtreeWriteStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, ws.Stat, "size").Set(float64(ws.Size))
		}
	}

	promBchSysFsBtreeCacheSize.WithLabelValues(fsUsage.Path, fsUsage.FileSystem).Set(float64(sysFs.BtreeCacheSize))

	if sysFs.CompressionStat != nil {
		for _, cs := range sysFs.CompressionStat {
			promBchSysFsCompressionStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, cs.CompressionType, "compressed").Set(float64(cs.Comporessed))
			promBchSysFsCompressionStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, cs.CompressionType, "uncompressed").Set(float64(cs.Uncompressed))
			promBchSysFsCompressionStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, cs.CompressionType, "average extent size").Set(float64(cs.AverageExtentSize))
		}
	}

	if sysFs.RebalanceStatus != nil {
		rs := sysFs.RebalanceStatus
		promBchSysFsRebalanceStatus.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, rs.State, rs.DataType, "keys moved").Set(float64(rs.KeysMoved))
		promBchSysFsRebalanceStatus.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, rs.State, rs.DataType, "keys raced").Set(float64(rs.KeysRaced))
		promBchSysFsRebalanceStatus.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, rs.State, rs.DataType, "bytes seen").Set(float64(rs.BytesSeen))
		promBchSysFsRebalanceStatus.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, rs.State, rs.DataType, "bytes moved").Set(float64(rs.BytesMoved))
		promBchSysFsRebalanceStatus.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, rs.State, rs.DataType, "bytes raced").Set(float64(rs.BytesRaced))
	}

	for k, v := range sysFsTimestats {
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "count").Set(float64(v.Count))
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_min").Set(v.Duration.Min)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_max").Set(v.Duration.Max)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_total").Set(v.Duration.Total)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_mean").Set(v.Duration.Mean)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_stddev").Set(v.Duration.Stddev)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_recent_mean").Set(v.Duration.RecentMean)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "duration_recent_stddev").Set(v.Duration.RecentStddev)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "interval_min").Set(v.Interval.Min)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "interval_max").Set(v.Interval.Max)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "interval_mean").Set(v.Interval.Mean)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "interval_stddev").Set(v.Interval.Stddev)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "interval_recent_mean").Set(v.Interval.RecentMean)
		promBchSysFsTimeStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "interval_recent_stddev").Set(v.Interval.RecentStddev)
	}

	for k, v := range sysFsDevs {
		promBchSysFsDevStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "bucket_size").Set(float64(v.BucketSize))
		promBchSysFsDevStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "nbuckets").Set(float64(v.NBuckets))
		promBchSysFsDevStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "first_bucket").Set(float64(v.FirstBucket))
		promBchSysFsDevStat.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "durability").Set(float64(v.Durability))
		for rK, rV := range v.IoDone.Read {
			promBchSysFsDevIoDone.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "read", rK).Set(float64(rV))
		}
		for wK, wV := range v.IoDone.Write {
			promBchSysFsDevIoDone.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "write", wK).Set(float64(wV))
		}
		promBchSysFsDevIoErrors.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "read").Set(float64(v.IoErrors.Read))
		promBchSysFsDevIoErrors.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "write").Set(float64(v.IoErrors.Write))
		promBchSysFsDevIoErrors.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, "checksum").Set(float64(v.IoErrors.Checksum))

		for i, ts := range []*sysfs.SysFsTimeStat{v.IoLatencyRead, v.IoLatencyWrite} {
			dir := ""
			if i == 0 {
				dir = "read"
			} else {
				dir = "write"
			}
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "count").Set(float64(ts.Count))
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_min").Set(ts.Duration.Min)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_max").Set(ts.Duration.Max)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_total").Set(ts.Duration.Total)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_mean").Set(ts.Duration.Mean)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_stddev").Set(ts.Duration.Stddev)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_recent_mean").Set(ts.Duration.RecentMean)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "duration_recent_stddev").Set(ts.Duration.RecentStddev)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "interval_min").Set(ts.Interval.Min)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "interval_max").Set(ts.Interval.Max)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "interval_mean").Set(ts.Interval.Mean)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "interval_stddev").Set(ts.Interval.Stddev)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "interval_recent_mean").Set(ts.Interval.RecentMean)
			promBchSysFsDevIoLatency.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, v.Uuid, v.Label, dir, "interval_recent_stddev").Set(ts.Interval.RecentStddev)
		}
	}

	for k, v := range sysFsCounters {
		promBchSysFsCounter.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "mount").Set(float64(v.Mount))
		promBchSysFsCounter.WithLabelValues(fsUsage.Path, fsUsage.FileSystem, k, "creation").Set(float64(v.Creation))
	}
	log.Infof("Parsed %s", fsUsage.FileSystem)
}

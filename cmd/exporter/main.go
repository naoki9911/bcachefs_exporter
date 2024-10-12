package main

import (
	"flag"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/naoki9911/bcachefs_exporter/pkg/bcachefs"
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
	log.Infof("Parsed %s", fsUsage.FileSystem)
}

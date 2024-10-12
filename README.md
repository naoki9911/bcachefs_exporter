# bcachefs_exporter
A prometheus exporter for bcachefs

# Install
bcachefs_exporter requires `bcachefs-tool`.
Please ensure `bcachefs` command is available.  
To build binary, Go is also required.
We have tested with `go1.23.2`

Before install, edit `bcachefs_exporter.service` to specify bcachefs mounted path.  
The default path is `/tank`

Metrics is available at `:9091/metrics`
```bash
$ bcachefs version
1.9.5
$ go version
go version go1.23.2 linux/amd64

$ make
$ sudo make install
$ sudo systemctl daemon-reload
$ sudo systemctl enable --now bcachefs_exporter.service

$ curl localhost:9091/metrics
# HELP bcachefs_fs_usage_btree 
# TYPE bcachefs_fs_usage_btree gauge
bcachefs_fs_usage_btree{dataType="accounting",mountpoint="/tank",uuid="XXX"} 5.88775424e+09
bcachefs_fs_usage_btree{dataType="alloc",mountpoint="/tank",uuid="XXX"} 1.1557404672e+10
...
```
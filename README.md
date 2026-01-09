# Slurm adapter for ASCOW/ASCOW Pro <!-- omit in toc -->

适用于ASCOW和ASCOW Pro系统的Slurm适配器。

- [1 功能更新](#1-功能更新)
- [2 安装](#2-安装)
  - [2.1 系统要求](#21-系统要求)
  - [2.2 编译](#22-编译)
  - [2.3 安装](#23-安装)


## 1 功能更新

**2025-09-06**
- 保存用户作业调取`Environment Module`记录，便于后期做软件使用分析。如用户主节点未配置`Environment Module`记录器，则记录值为`unknown`，需要`ASCOW`版本>=`20250906`。效果如下图。
![1757250643455](image/README/1757250643455.png)

## 2 安装
### 2.1 系统要求

[Buf](https://buf.build/docs/installation/)

[Git>=2.18.0](https://github.com/git/git)

[golang](https://go.dev/doc/install)

### 2.2 编译
```bash
module load git/2.18.0

# Generate code from latest scow-slurm-adapter
make protos

# Build
make build
```

### 2.3 安装
推荐放到```/opt/scow-slurm-adapter/```目录下(看个人习惯)，以下脚本根据实际情况修改：
```bash
#! /bin/bash
set -e

DES="/opt/scow-slurm-adapter"   # 这里定义安装目录

# 安装scow adapter
mkdir $DES/config -p
mv scow-slurm-adapter-amd64 $DES/scow-slurm-adapter

# 写入配置文件，这里根据实际情况修改
cat << EOF > $DES/config
# slurm 数据库配置
mysql:
  host: 127.0.0.1
  port: 3306
  user: root
  dbname: slurm_acct_db
  password: 81SLURM@@rabGTjN7
  clustername: cluster
  databaseencode: utf8

# 服务端口设置
service:
  port: 8972

# slurm 默认Qos设置
slurm:
  defaultqos: normal
  # 指定Slurm安装目录；如不指定改行，则默认slurm安装在/usr目录下，sbatch等命令的全路径为/usr/bin/sbatch
  slurmpath: /home-beegfs/apps/slurm

# module profile文件路径
modulepath:
  path: /lustre/software/module/5.2.0/init/profile.sh
EOF


# 写入systemd服务
cat << EOF > /etc/systemd/system/scow-adapter.service
[Unit]
Description=SCOW SLURM Adapter Service
After=network.target

[Service]
WorkingDirectory=/opt/scow-slurm-adapter/
ExecStart=/opt/scow-slurm-adapter/scow-slurm-adapter
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now scow-adapter.service
```

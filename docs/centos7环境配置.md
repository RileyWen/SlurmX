# centos7 Slurmx环境配置

## 环境准备

安装ntp ntpdate同步时钟
```shell script
yum install ntp ntpdate
systemctl start ntpd
systemctl enable ntpd

timedatectl set-timezone Asia/Shanghai

```

关闭防火墙，不允许关闭防火墙则考虑开放10011、10010、873端口
```shell script
systemctl stop firewalld
systemctl disable firewalld


# 或者开放端口
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
#重启防火墙(修改配置后要重启防火墙)
firewall-cmd --reload
```
## 安装工具链
安装C++11
```shell script
# Install CentOS SCLo RH repository:
yum install centos-release-scl-rh
# Install devtoolset-11 rpm package:
yum install devtoolset-11
# 第三步就是使新的工具集生效
scl enable devtoolset-11 bash
```
这时用gcc --version查询，可以看到版本已经是11.2系列了
```shell script
$ gcc --version
gcc (GCC) 11.2.1 20210728 (Red Hat 11.2.1-1)
Copyright (C) 2021 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
```

为了避免每次手动生效，可以在.bashrc中设置：
```shell script
$ source /opt/rh/devtoolset-11/enable
or
$ source scl_source enable devtoolset-11
```

给系统安装个下载命令器
```shell script
yum install wget -y
```
安装cmake和ninja
由于需要源码安装，首先选择合适的源码存放位置，可以放在自己账号的目录下。
```shell script
su - liulinxing # 用户名
mkdir download
cd download
```
从github下载源码
```shell script
wget https://github.com/ninja-build/ninja/releases/download/v1.10.2/ninja-linux.zip
wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3.tar.gz
```

解压编译安装
```shell script
unzip ninja-linux.zip
cp ninja /usr/bin/

yum install openssl-devel

tar -zxvf cmake-3.21.3.tar.gz
cd cmake-3.12.4
./bootstrap
gmake
gmake install
```

检查安装是否成功
```shell script
cmake --version
#cmake version 3.21.3
#
#CMake suite maintained and supported by Kitware (kitware.com/cmake).
```

## 安装C++库

复制相关源码压缩包到一个目录下


解压所有源码压缩包
```shell script
tar xzf boost_1_78_0.tar.gz
tar xzf fmt-8.0.1.tar.gz
tar xzf libevent-2.1.12.tar.gz
tar xzf cxxopts-2.2.1.tar.gz
tar xzf googletest-1.10.0.tar.gz
tar xzf libuv-1.42.0.tar.gz
tar xzf spdlog-1.8.5.tar.gz
```

安装第三方库
```shell script
yum install libcgroup-devel
yum install libcurl-devel

 cd boost_1_78_0
./bootstrap.sh
./b2 install --with=all

cd ..
cd libuv-1.42.0
mkdir build
cd build/
cmake -DCMAKE_INSTALL_PREFIX=/nfs/home/testSlurm/SlurmX/dependencies/online/libuv -DCMAKE_CXX_STANDARD=17 -G Ninja ..
ninja install

# 运行安装脚本
bash ./download_deps.sh
```

## 安装mariadb

通过yum安装就行了，安装mariadb-server，默认依赖安装mariadb，一个是服务端、一个是客户端。
```shell script
wget https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
chmod +x mariadb_repo_setup
./mariadb_repo_setup
yum install MariaDB-server
yum install mysql-devel #安装链接库

systemctl start mariadb  # 开启服务
systemctl enable mariadb  # 设置为开机自启动服务

mariadb-secure-installation  # 首次安装需要进行数据库的配置
```
配置时出现的各个选项
```shell script
Enter current password for root (enter for none):  # 输入数据库超级管理员root的密码(注意不是系统root的密码)，第一次进入还没有设置密码则直接回车

Set root password? [Y/n]  # 设置密码，y

New password:  # 新密码  123456
Re-enter new password:  # 再次输入密码:123456

Remove anonymous users? [Y/n]  # 移除匿名用户， y

Disallow root login remotely? [Y/n]  # 拒绝root远程登录，n，不管y/n，都会拒绝root远程登录

Remove test database and access to it? [Y/n]  # 删除test数据库，y：删除。n：不删除，数据库中会有一个test数据库，一般不需要

Reload privilege tables now? [Y/n]  # 重新加载权限表，y。或者重启服务也许
```

安装过程中遇到Table doesn’t exist报错：
ERROR 1146 (42S02) at line 1: Table ‘mysql.global_priv’ doesn’t exist … Failed!
执行下面的命令后，重启mysql
```shell script
mysql_upgrade -uroot -p --force
```
重启数据库
```shell script
systemctl restart mariadb
```
登陆数据库
```shell script
mysql -uroot -p
Enter password:
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Your MariaDB connection id is 9
Server version: 10.4.8-MariaDB MariaDB Server
```
进入mysql数据库
```shell script
use mysql
```
查询user表，可看到多条数据
```shell script
select host,user,password from user;
```
删除localhost以外数据
```shell script
delete from user where host !='localhost';
```
配置完毕，退出
```shell script
exit;
systemctl restart mariadb
```

## 编译程序
首先进入到项目目录下
```shell script
mkdir build
cd build/

cmake -DCMAKE_CXX_STANDARD=17 -G Ninja ..
cmake --build .
```


## 配置前端go语言环境

安装go语言
```shell script
cd download/
wget https://golang.google.cn/dl/go1.17.3.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.17.3.linux-amd64.tar.gz

# 在 /etc/profile中设置环境变量
export PATH=$PATH:/usr/local/go/bin

source /etc/profile

go version

#设置代理
go env -w GOPROXY=https://goproxy.cn,direct 

# 安装插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

cp /root/go/bin/protoc-gen-go-grpc /usr/local/bin/
cp /root/go/bin/protoc-gen-go /usr/local/bin/

```
安装protuc
```shell script
https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protobuf-all-3.19.4.tar.gz
tar -xzf protobuf-all-3.19.4.tar.gz
cd protobuf-3.19.4
./configure -prefix=/usr/local/
make && make install
protoc --version
# libprotoc 3.11.2
```

拉取项目
```shell script
git clone https://github.com/RileyWen/SlurmX-FrontEnd.git # 克隆项目代码

mkdir SlurmX-FrontEnd/out
mkdir SlurmX-FrontEnd/generated/protos
```

编译项目
```shell script
# 在SlurmX-FrontEnd/protos目录下
protoc --go_out=../generated --go-grpc_out=../generated ./*

# 在SlurmX-FrontEnd/out目录下
go build Slurmx-FrontEnd/cmd/sbatchx/sbatchx.go
```

部署前端命令
```shell script
ln -s /nfs/home/testSlurmX/SlurmX-FrontEnd/out/sbatchx /usr/bin/sbatchx
ls -s /nfs/home/testSlurmX/SlurmX-FrontEnd/out/scontrol /usr/bin/scontrol
```

## 编写系统服务
```shell script
vim /etc/systemd/system/slurmctlxd.service

#####内容如下######
[Unit]
Description=SlurmCtlXd
After=network.target nss-lookup.target

[Service]
User=root
CapabilityBoundingSet=CAP_NET_ADMIN CAP_NET_BIND_SERVICE
AmbientCapabilities=CAP_NET_ADMIN CAP_NET_BIND_SERVICE
NoNewPrivileges=true
ExecStart=/nfs/home/testSlurmX/SlurmX/build/src/SlurmCtlXd/slurmctlxd

[Install]
WantedBy=multi-user.target


vim /etc/systemd/system/slurmxd.service

# 内容类似

# 在节点启动相应服务
systemctl start slurmctlxd
systemctl start slurmxd
```

### 部署文件同步

在所有节点安装rsync
```shell script
yum -y install rsync

# 安装完成后，使用rsync –-help命令可查看 rsync 相关信息
```

在SlurmCtlxd安装inotify
```shell script
yum install -y epel-release
yum --enablerepo=epel install inotify-tools
```

编写监听脚本，并在后台自动运行
```shell script
 vim /etc/slurmx/inotifyrsync.sh

#####
inotifywait -mrq --timefmt '%d/%m/%y %H:%M' --format '%T %w%f' -e modify,delete,create,attrib /etc/slurmx/ | while read file
do
        for i in {cn01,cn02,cn03,cn04,cn05,cn06,cn07,cn08,cn09,cn10}
        do
                rsync -avPz --progress --delete /etc/slurmx/ $i:/etc/slurmx/
        done
echo "${file} was synchronized"
done
########

chmod 755 /etc/slurmx/inotifyrsync.sh

/etc/slurmx/inotifyrsync.sh &
echo "/etc/slurmx/inotifyrsync.sh &" >> /etc/rc.local
```
